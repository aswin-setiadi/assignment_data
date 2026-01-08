# Canonical Email Thread Deduplication Assignment

## Observation

- From eval directory, there seemed to be at most 1 variation for each canonical thread.

- There is no date information for each email.

- Some user has no explicit email address.

- A preceeding slightly modified thread will have the exact same variation in the next thread chain e.g. 96_1m, 96_1m_2, 96_1m_2m, 1m amongst the 3 are the same

- There seemed to be no variation in the subject

- It seems 1 user name map to 1 email

- It seems we can roughly estimate the length of the thread in a .txt by splitting by "From:"

- It seems the set of email addreses involved are the same in threads of a canonical thread.

- There is no quoted history

- Some email has subject and body seperated with ---

## Expectation

1. Build data ingestion pipeline.
2. Build api that allow queries based on Implementation Requirements.

## Analysis

- It seems when an email thread has started, the subject will have variation of Re: and Fwd:
- It seems every email thread variations have the same combination of From: To CC: Subject: (those lines start with them). then email body. The samples also doesn't have space between the word and :.
- Email stream may come not in order e.g. 6_1_2 -> 6 -> 6_1
- We don't know if an email belong to which canonical thread, so we will not use kafka message key
- To make the same raw document getting stored, we filter the upsert against documentid/ filename.

## Implementation

1. Mount the sample email folder (eval or test) minikube mount e.g. ./samples:test (currently minikube QEMU does not support so I upload it to the producer docker image).
2. Create a kafka producer with a topic and maximum partition count (I use 30 for this assignment) that cycle the payload (read from folder producer/data/eval|test) endlessly to simulate real life streaming.
3. Consumer group will consume each payload, save to mongodb database: email (for eval)/ test  collection: canonicalthread and rawemail.
4. Each thread will be grouped by combination of document (row in sql) attribute Subject+rank where Subject is thread first email subject, rank is email count in the thread.
5. Should an update fail on a given payload (consumer raised exception, db down, etc.), instead of keep reconsuming, in the catch exception block, send the payload to dead letter queue topic and commit the offset. Set notification tool (slack, email, etc.) should the DLQ get sudden message spikes for monitoring.
6. Don't assign key to producer message since the grouping by subject members are relatively small. This means message key is None which allows kafka to use round-robin or sticky partitioning to distribute load more evenly across all consumers/ partitions, preventing "hot partitions" where one consumer is overloaded.
7. In the event 2 producer send 2 payload with same doc_id for the first time at the same time, the upsert operation might insert 2 raw email to `rawemail` collection (but `$addToSet` prevent adding duplicate doc_id to **doc_ids** attribute of `canonicalthread` collection). This will be prevented by introducing unique index on `rawemail` collection.

## Deployment

- $ checkout master branch
- $ minikube start
- $ minikube dashboard
- $ eval $(minikube docker-env)
- $docker build -t email-producer:latest ./producer
- $docker build -t email-consumer:latest ./consumer
- $docker build -t fastapi-email:latest ./api
- $ minikube kubectl -- apply -f k8s/kafka.yaml
- $ minikube kubectl -- apply -f k8s/mongodb.yaml
- $ minikube kubectl -- apply -f k8s/api.yaml
- $ minikube kubectl -- apply -f k8s/consumer.yaml
- $ minikube kubectl -- apply -f k8s/producer.yaml
- $ minikube kubectl -- port-forward svc/kafka 9092:9092
- $ minikube kubectl -- port-forward svc/mongodb-service 27017:27017
- $ minikube kubectl -- port-forward svc/fastapi-email 8000:80

## Improvements

- (Weakness) Current implementation if 2 thread have same subject but different body/ topic, the canonical thread mapping will fail. Solution: fuzzy hash subject+normalized body(lowercased+replace \s with " ") to replace the canon key
- From quick google search, hamming distance seemed to be able to make fuzzy hashing, grouping threads that are slightly different/ near-duplicates
- Hash subject for easier search (query parameter no need to be encoded)
- (Weakness) To detect wether a document is the raw email or the modified version:
  - If we can assume non ascii character exist == modified version, we just scan each character in a document until a non ascii character is found -> label with m suffix
  - Else, if a typo can be non non-ascii character e.g. Python-> Ptyhon, we need to have a set obj for english words, split the email threads body into words, lowercase them, and check if it is in the set obj (constant runtime/ n where n is number of email thread word). Label with suffix m if at least 1 word not in the dictionary. However some brand can be a typo of real English word, i.e. Lyft. In this case we may need to apply NLP techniques.

## Usefule minikube commands

- minikube start
- eval $(minikube docker-env) # to alias docker command
- docker build -t email-producer:latest ./producer
- docker build -t email-consumer:latest ./consumer
- docker build -t fastapi-email:latest ./api
- minikube image ls
- minikube dashboard
- minikube mount /abs/path/to/data:/opt/data #persist data to local computer, not implemented in minikube QEMU
- minikube kubectl -- \<kubectl commands\>
- minikube kubectl -- get svc
- minikube kubectl -- exec -it kafka-0 -- bash
- minikube kubectl -- apply -f k8s/kafka.yaml
- minikube kubectl -- apply -f k8s/mongodb.yaml
- minikube kubectl -- apply -f k8s/producer.yaml
- minikube kubectl -- apply -f k8s/consumer.yaml
- minikube kubectl -- apply -f k8s/api.yaml
- minikube kubectl -- delete -f mongodb.yaml
- minikube kubectl -- port-forward svc/kafka 9092:9092
- minikube kubectl -- port-forward svc/mongodb-service 27017:27017
- minikube kubectl -- port-forward svc/fastapi-email 8000:80
- minikube service list
- minikube kubectl -- get pods
- minikube kubectl -- logs -f (pod-name)

## Useful kafka commands

- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic email_threads --from-beginning
- ./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list --state #See list of consumer group
