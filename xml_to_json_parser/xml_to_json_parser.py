import multiprocessing as mp
import re
from xmljson import Yahoo
from xml.etree.ElementTree import fromstring
import ujson
import html

XML_PATH = './dblp.xml'
JSON_PATH = './dblp.json'

XML_PARSER = Yahoo()

FACTOR_BATCH = 10000
BATCH_SIZE = mp.cpu_count() * FACTOR_BATCH 

def to_json(xml_string):
    try:
        return ujson.dumps(XML_PARSER.data(fromstring(xml_string)))
    except Exception as e:        
        return '{"error":{}}'

def process_batch(json_file, ok, error, batch):
    jsons = pool.map(to_json, batch)

    for json in jsons:
        if json != '{"error":{}}':
            json = str(ujson.loads(json))
            if ok > 0:
                json_file.write(',' + json)
            else:
                json_file.write(json)
            ok += 1
        else:            
            error += 1 

    json_file.flush()
    jsons.clear()
    batch.clear()
            
    return {'ok': ok, 'error': error}

def when_found_open_tag(matched, tags):
    tags += 1
    matched_tag = matched_aux.group() 

    if matched_tag == '<article':
        end_tag_to_find = '</article>'
    elif matched_tag == '<improceedings':
        end_tag_to_find = '</improceedings>'
    else:
        end_tag_to_find = '</incollection>'

    return {'rgx_close': re.compile(end_tag_to_find), 'tags': tags}    

if __name__ == '__main__':
    with open(XML_PATH, 'r') as f:
        json_file = open(JSON_PATH, 'w+')
        json_file.write('[')

        ok_and_error = {'ok': 0, 'error': 0}
        tags = 0
      
        matched_tag = ''
        rgx_close = None

        rgx_open = re.compile('(\A<article)|(\A<improceedings)|(\A<incollection)')
        rgx_open_on_close_line = re.compile('(<article)|(<improceedings)|(<incollection)')

        batch = []

        current_element = ''

        pool = mp.Pool(mp.cpu_count())

        for line in f: 
            # when a tag from the previous rgx is found, that line and the following lines (included the one with the ending tag) are stored
            matched_aux = rgx_open.match(line)

            if matched_aux: #if a tag is found...
                rgxclose_and_tags = when_found_open_tag(matched_aux, tags)

                rgx_close = rgxclose_and_tags['rgx_close']
                tags = rgxclose_and_tags['tags']

                current_element = line
            elif rgx_close: #when no tag has been found in the current line but we're looking for an ending tag...                  
                if not rgx_close.match(line): # if there's no ending tag in the current line, that means we are in a line between the opening and the ending tags         
                    current_element = current_element + line
                else: # if there's an ending tag in the current line...
                    current_element = current_element + rgx_close.pattern

                    batch.append(html.unescape(current_element).replace('&', '&#038;')) # including element in the batch removing weird characters 

                    rgx_close = None
                    current_element = ''

                    if len(batch) == BATCH_SIZE: # the batch is processed when it reaches its limit size
                        ok_and_error = process_batch(json_file, ok_and_error['ok'], ok_and_error['error'], batch)

                    # an opening tag might be found in the same line where there's an ending tag
                    matched_aux = rgx_open_on_close_line.search(line)

                    if matched_aux: #if a tag is found...
                        rgxclose_and_tags = when_found_open_tag(matched_aux, tags)

                        rgx_close = rgxclose_and_tags['rgx_close']
                        tags = rgxclose_and_tags['tags']

                        # the opening tag is saved as part of the current element. Note the whole line isn't stored
                        current_element = line[matched_aux.start():len(line)]

        #There might be remaining elements to process in the batch
        ok_and_error = process_batch(json_file, ok_and_error['ok'], ok_and_error['error'], batch)

        json_file.write(']')
        json_file.close()
        
        print('Errors %d' % ok_and_error['error'])
        print('Ok %d' % ok_and_error['ok'])
        print('Tags %d' % tags)