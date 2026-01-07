from pathlib import Path
import re

def check_thread_no_cc():
    encodings=("utf-8", "cp1252", "latin-1", "iso-8859-1")
    folder_path= Path("test")
    count=0
    no_cc=0
    for file in folder_path.iterdir():
        if file.is_file():
            for enc in encodings:
                try:
                    with open(file.absolute(), "r", encoding="latin-1") as f:
                        content= f.read()
                        break
                except Exception as e:
                    if enc==encodings[-1]:
                        print(f"all enc fail for {file.name} continue")
                        continue
            prev= None
            curr= None
            count+=1
            nocc= True
            for line in content.splitlines():
                if (curr and prev and curr.lstrip().lower().startswith("cc:")
                    and prev.lstrip().lower().startswith("to:")
                    and line.lstrip().lower().startswith("subject:")):
                    nocc= False
                prev, curr= curr, line
            if nocc:
                print(file.name)
                no_cc+=1
    print(f"ends {count=} {no_cc=}")

def regex_test():
    pattern=r"^Subject:\s*(re|fw|fwd)\s*:\s*"
    pattern=r'^((Re|Fwd):\s*)+'
    p=re.compile(pattern, re.I)
    s1="Review of Q2 Fiscal Strategies"
    s2="Re: Review of Q2 Fiscal Strategies"
    s3="Re: Fwd: Re: Review of Q2 Fiscal Strategies"
    s4="Fwd: Re: Re: Review of Q2 Fiscal Strategies"
    m1= re.search(p, s1)
    m2= re.search(p, s2)
    m3= re.search(p, s3)
    m3a= re.findall(p, s3)
    m4= re.search(p,s4)
    clean1= re.sub(pattern, "", s1, flags=re.I)
    clean2= re.sub(pattern, "", s2, flags=re.I)
    clean3= re.sub(pattern, "", s3, flags=re.I)
    clean3= re.sub(pattern, "", s3, flags=re.I)
    print("ends")
if __name__=="__main__":
    # check_thread_no_cc() #1330, 0= all email threads have cc
    regex_test()