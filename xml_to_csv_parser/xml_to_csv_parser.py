import csv
from lxml import etree
from operator import itemgetter
from itertools import groupby
import time

xml = 'may/dblp.xml'
parser = etree.iterparse(source=xml, dtd_validation=True, load_dtd=True)

INTEREST_PUBS=set(['article', 'incollection', 'inproceedings'])
ALLOWED_FIELDS = ["pubtype", "mdate", "booktitle", 
                  "year", "url", "author", 
                  "title", "key"]


def check_entry_dict(entry):
    isvalid = True
    try:
        year = entry['year']
        if not len(year) > 0:
            isvalid = False
    except:
        isvalid = False

    return isvalid


def xml_entry_as_dict(name, key, mdate, elements=[]):
    elements_keys = set([k for k,v in elements])

    entry_dict={}
    entry_dict['pubtype'] = name
    entry_dict['mdate'] = mdate
    entry_dict['key'] = key
    for k,g in groupby(elements, itemgetter(0)):
        if k in ALLOWED_FIELDS:
            groups = list(g)
            if len(groups) == 1:
                entry_dict[k] = groups[0][1]
            else:
                entry_dict[k] = ';'.join([ v for k,v in groups ])
    isvalid = check_entry_dict(entry_dict)
    return (entry_dict, isvalid)


if __name__ == "__main__":
    t0 = time.time()
    lines = 0
    valid_lines = 0
    with open('dblp-full-2.csv','w') as f:
        csvwriter = csv.DictWriter(f, fieldnames = ALLOWED_FIELDS)
        csvwriter.writeheader()
        for _, elem in parser:
            lines = lines + 1
            if lines % 1000000 == 0:
                print("Processed {0} lines. {1} valid. Took {2:0.2f} secs".format(lines, valid_lines, time.time()-t0))

            if elem.tag in INTEREST_PUBS:
                valid_lines = valid_lines + 1

                key = elem.attrib['key']
                mdate = elem.attrib['mdate']


                elements = []
                for e in elem:
                    if e.text:
                        text = e.text.replace('"',' ')
                    else:
                        text = "NA"
                    elements.append((e.tag,text))
                entry_dict, isvalid = xml_entry_as_dict(elem.tag, key, mdate, elements)
                if isvalid:
                    csvwriter.writerow(entry_dict)
            else:
                continue
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
                       #elem.clear()
            #while elem.getprevious() is not None:
            #    del elem.getparent()[0]
    print("Finish. Total Lines:{0}. Valid Lines:{1}. Total Time: {2:0.2f} secs".format(lines, valid_lines, time.time()-t0))
