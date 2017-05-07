import multiprocessing as mp
import re
from lxml import etree
import ujson
import html

XML_PATH = './dblp.xml'
JSON_PATH = './dblp.json'

FACTOR_BATCH = 10000
BATCH_SIZE = mp.cpu_count() * FACTOR_BATCH 

context = etree.iterparse(XML_PATH, dtd_validation=False, load_dtd=True, events=('start', 'end'), encoding='ISO-8859-1')

types = set(['article', 'inproceedings', 'incollection'])

def process_batch(batch, json_file, count):
    for json in batch:
        if count > 0:
            json_file.write(',' + json)
        else:
            json_file.write(json)   

        count += 1   

    json_file.flush()
    batch.clear()  

    return count

if __name__ == '__main__':
    parse_doc = False
    parse_author_doc = False

    batch = []

    current_doc = {'author': [], 'title': ''}

    count = 0

    json_file = open(JSON_PATH, 'w+', encoding='UTF-8')
    json_file.write('[')

    for event, element in context:
        if event == 'start':
            interesting_root_tag = element.tag in types

            if interesting_root_tag or parse_doc:
                if interesting_root_tag:
                    parse_doc = True
                    current_doc['type'] = element.tag
                elif element.tag == 'author':
                    parse_author_doc = True
                    current_doc['author'].append(str(element.text))
                elif element.tag == 'content' and parse_author_doc: # author stuff
                    current_doc['author'][-1] = element.text
                elif element.tag == 'year':
                    if str(element.text).isdigit():
                        current_doc['year'] = int(element.text)
                elif element.tag == 'title':
                    current_doc['title'] = element.text

        else:
            if element.tag in types:
                parse_doc = False

                doc_str = (str(ujson.dumps(current_doc, 
                                           escape_forward_slashes=False, 
                                           encode_html_chars=False, 
                                           ensure_ascii=False)))       

                batch.append(doc_str)         

                current_doc = {'author': [], 'title': ''}
                element.clear()
                
                # xml tree parser frees memory
                while element.getprevious() is not None:
                    del element.getparent()[0]
                
                len_batch = len(batch)
                if len_batch == BATCH_SIZE:
                    count = process_batch(batch, json_file, count)

            elif element.tag == 'author':
                parse_author_doc = False


    count = process_batch(batch, json_file, count)

    json_file.write(']')
    json_file.close()

    print('Documents: %d' % count)