# -*- coding: utf-8 -*-
"""
Created on Wed May 17 11:19:26 2017

@author: Hariharan
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
To do so you will parse the elements in the OSM XML file, transforming them from document format to
tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

We've already provided the code needed to load the data, perform iterative parsing and write the
output to csv files. Your task is to complete the shape_element function that will transform each
element into the correct format. To make this process easier we've already defined a schema (see
the schema.py file in the last code tab) for the .csv files and the eventual tables. Using the 
cerberus library we can validate the output against this schema to ensure it is correct.

## Shape Element Function
The function should take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
               
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': '209809850',
               'key': 'street:prefix',
               'type': 'addr',
               'value': 'West'},
              {'id': 209809850,
               'key': 'street:type',
               'type': 'addr',
               'value': 'Street'},
              {'id': 209809850,
               'key': 'building',
               'type': 'regular',
               'value': 'yes'},
              {'id': 209809850,
               'key': 'levels',
               'type': 'building',
               'value': '1'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET
import cerberus
import schema
import time


start = time.time()
#perform all imports to be used in the analysis
#Probably more than I need
OSM_PATH = "Berlin_new.osm.xml" #provide the osm file name in the same directory 
NODES_PATH = "nodes.csv" #csv files written from dictionaries created with formats above
NODE_TAGS_PATH = "nodes_tags.csv" #these will be used to create the SQL database
WAYS_PATH = "ways.csv" #they will be joint using IDs preferably 
WAY_NODES_PATH = "ways_nodes.csv" #never got to this 
WAY_TAGS_PATH = "ways_tags.csv" 

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+') 
#I am supposed to use this but not sure how
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]') #
#same as above here

SCHEMA = schema.schema 

# to Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

#this fucntion will be used to spit out the values of the "node" dictionaries

# ================================================== #
#              Main Shaping Function                 #
# ================================================== #

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""
    """Loads of arguments in the function not sure if I need em all""" 
    
    #tree = ET.parse(OSM_PATH) #making a tree from the OSM path that can be iterated using ETree
    node_attribs_val ={}
    way_attribs_val ={}
    node_tags = {}
    way_nodes = []
    way_nodes_dict = {}
    tags=[]
    way_tags = []
    way_tags_dict = {}
    #node_tags_audit = {}
    #for value in tree.iter(): #for each interation of the file
    if element.tag == "node" : #if the value of the tag is "node"
            for tag in element.iter("node"): #get other tags within the tag == "node"
            
                # I understand that the fucntion call is quite long. Will try to make it shorter next time onwards.
                # if I make the change a lot will have to be modified in the code probably breaking it.
                
                node_attribs_val.update(add_node_type(tag, tag.attrib['id'], tag.attrib['user'],tag.attrib['lat'],tag.attrib['lon'], tag.attrib ['uid'], tag.attrib['version'],tag.attrib['changeset'],tag.attrib['timestamp']))
                
                #this function takes the attributes from the tag- like id, user etc and makes a dictionary out of it
                #print node_attribs_val
            id_val = tag.attrib['id']
            #print id_val
            for tag in element.iter("tag"):
                #print "true"
                if is_street_name(tag) :
                    #checking if the street elements are what we want and then adding it to the node tags
                    node_tags = add_node_tags(tag, id_val, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    tags.append(node_tags)
                
                elif is_postcode(tag):
                    node_tags = add_node_tags(tag, id_val, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    #print node_tags
                    tags.append(node_tags)
                
                elif is_phone(tag):
                    phone = correct_phone(tag)
                    node_tags = add_node_tags(tag, id_val, tag.attrib['k'], phone, default_tag_type)
                    tags.append(node_tags)
                
                elif is_name(tag):
                    name = get_name(tag)
                    node_tags = add_node_tags(tag, id_val, tag.attrib['k'], name, default_tag_type)
                    tags.append(node_tags)
                    
                elif is_amenity(tag):
                    node_tags = add_node_tags(tag, id_val, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    tags.append(node_tags)
                    
                elif is_addr_city(tag):
                    node_tags = add_node_tags(tag, id_val, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    tags.append(node_tags)
                    
                elif is_suburb(tag):
                    node_tags = add_node_tags(tag, id_val, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    tags.append(node_tags)        
                    
                elif is_addr_housenumber(tag):
                    node_tags = add_node_tags(tag, id_val, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    tags.append(node_tags)
                
                elif is_addr_housename(tag):
                    node_tags = add_node_tags(tag, id_val, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    tags.append(node_tags)     
                    
                else:
                    pass
                #§ node_tags
                #else:
                #    pass
                #print node_tags
                #tags.append(node_tags)
                #print tags
                #print id_val, tag.attrib['k'], tag.attrib['v'], default_tag_types
                # this is used to store the value of id for the subtag "tag" within "node"
            #this function takes the tags attributes like "k" and "v" values and makes a dictionray out of this.       
    elif element.tag =="way":
        
        for tag in element.iter("way"):
                #this function takes the attributes from the "way" like id, user etc and makes  a dictiornary
                way_attribs_val = add_way_type(tag, tag.attrib["id"], tag.attrib["user"], tag.attrib["uid"], tag.attrib["version"], tag.attrib["changeset"], tag.attrib["timestamp"])
                #print way_attribs_val
                #keeping this value for adding it to the way_nodes dict
                #if value.tag =="nd":
                way_id = tag.attrib["id"]
        
        position = 0
        #print way_id, position
        
        for tag in element.iter("nd"):
                #print tag, way_id, tag.attrib["ref"], position
                temp_dict ={}
                if position ==0 :
                    temp_dict = add_way_nodes(tag, way_id, tag.attrib["ref"], position)
                    temp_dict.update({"position" : 0})
                    way_nodes_dict = temp_dict
                else:
                    way_nodes_dict = add_way_nodes(tag, way_id, tag.attrib["ref"], position)
                
                #print way_nodes_dict
                
                position += 1
                #print way_nodes_dict
                way_nodes.append(way_nodes_dict)
        way_id2 = way_id        
        #if value.tag == "tag" :
            
        for tag in element.iter("tag"):
                #print tag, way_id2, tag.attrib['k'], tag.attrib['v'], default_tag_type
                if is_street_name(tag):
                    way_tags_dict = add_way_tags(tag, way_id2, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    #print "street name" ,  tag.attrib['v']
                    way_tags.append(way_tags_dict)

                elif is_postcode(tag):
                    way_tags_dict = add_way_tags(tag, way_id2, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    #print "postcode",  tag.attrib['v']
                    way_tags.append(way_tags_dict)    
                    
                elif is_phone(tag):
                    phone = correct_phone(tag)
                    way_tags_dict = add_way_tags(tag, way_id2, tag.attrib['k'], phone, default_tag_type)
                    #print " phone " , phone
                    way_tags.append(way_tags_dict)         
                    
                elif is_name(tag):
                    way_tags_dict = add_way_tags(tag, way_id2, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    #print"name" ,  tag.attrib['v']
                    way_tags.append(way_tags_dict)  
                    
                elif is_amenity(tag):
                    way_tags_dict = add_way_tags(tag, way_id2, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    #print " amentiy " ,  tag.attrib['v']
                    way_tags.append(way_tags_dict)  
                    
                elif is_addr_city(tag):
                    way_tags_dict = add_way_tags(tag, way_id2, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    #print "address",  tag.attrib['v']
                    way_tags.append(way_tags_dict)  
                
                elif is_way_postcode(tag):
                    way_tags_dict = add_way_tags(tag, way_id2, tag.attrib['k'], tag.attrib['v'].encode('ascii','ignore'), default_tag_type)
                    #print "way_postcode" , tag.attrib['v']
                    way_tags.append(way_tags_dict) 
                
                else:
                    pass
    else:
            pass
        
    if element.tag == 'node':
        return {'node': node_attribs_val, 'node_tags': tags}

    elif element.tag == 'way':
        return {'way': way_attribs_val, 'way_nodes': way_nodes, 'way_tags': way_tags}

#all the values with street names should be scanned and appended 
# fucntion check if the values are street names

# ================================================== #
#               Tag / Value auditing fucntions                  #
# ================================================== #
def get_name(elem):
    return (elem.attrib['v']).encode('ascii','ignore')
    
def is_way_postcode(elem):
    return (elem.attrib['k'] == "post_code")

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")
    
def is_postcode(elem):
    return (elem.attrib['k'] == "addr:postcode") and re.match(r'^\d{5}$', elem.attrib['v'])
                
def is_phone(elem):
    return (elem.attrib['k'] == "contact:phone" )

def is_addr_housenumber(elem):
    return (elem.attrib['k'] == "addr:housenumber" )
    
def is_addr_housename(elem):
    return (elem.attrib['k'] == "contact:housename")   
    
def is_addr_city(elem):
    return (elem.attrib['k'] == "addr:city" )
    
def is_name(elem):
    return (elem.attrib['k'] == "name")                    
                    
def is_amenity(elem):
    return (elem.attrib['k'] == "amenity")
    
def is_suburb(elem):
    return (elem.attrib['k'] == "addr:suburb")
    

    
def correct_phone(elem):

    if '{}'.format(elem.attrib['v'])[0] == '+':
        return elem.attrib['v']

    elif '{}'.format(elem.attrib['v'])[0] == '0' :
        oldphone  = '{}'.format(elem.attrib['v'])
        newphone = '+49' + oldphone[1:]
        print oldphone,newphone
        return newphone
        
    elif '{}'.format(elem.attrib['v'])[0] == '4' :
        print elem.attrib['v'], '+'+ elem.attrib['v'] 
        return '+'+ elem.attrib['v']

    elif '{}'.format(elem.attrib['v'])[0] == '3' :
        print elem.attrib['v'], '+49'+ elem.attrib['v'] 
        return '+49'+ elem.attrib['v']

    elif '{}'.format(elem.attrib['v'])[0] == '-':
        oldphone  = '{}'.format(elem.attrib['v'])
        newphone = '+49' + oldphone[1:]
        print oldphone,newphone
        return newphone
        
    elif '{}'.format(elem.attrib['v'])[1] == '.' :
        oldphone  = '{}'.format(elem.attrib['v'])        
        newphone = '+4' + oldphone[2:]
        print oldphone,newphone
        return newphone

    else:
        pass
         
# ================================================== #
#               Dictionary building fucntions                  #
# ================================================== #

def add_way_type(tag, id, user, uid, version, changeset, timestamp ):
    
    way_attribs = {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
        
    if id:
        way_attribs.update({"id": int(id)})
    if user:
        way_attribs.update({"user": user})
    if uid:
        way_attribs.update({"uid": int(uid)})
    if version:
        way_attribs.update({"version": str(version)})
    if changeset:
        way_attribs.update({"changeset": int(changeset)})
    if timestamp:
        way_attribs.update({"timestamp": str(timestamp)})

    else:
        pass

    return way_attribs 
         
     
def add_way_nodes(tag, id, node_id, position):
    way_nodes2 = {}         
            
    if id :
        way_nodes2["id"] = int(id)
    if node_id:
        way_nodes2["node_id"] = int(node_id)
    if position:
        way_nodes2["position"]= position  
    else:
        pass
    
    return way_nodes2

     
def add_way_tags(tag, id, key, value, default_tag_type):
    
    way_tags2 = {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'key': {'required': True, 'type': 'string'},
                'value': {'required': True, 'type': 'string'},
                'type': {'required': True, 'type': 'string'}
            }
    if id:
        way_tags2.update({"id": int(id) })
    if key:
        way_tags2.update({"key" : str(key)})
    if value:
        way_tags2.update({"value": value})
    if type: 
        way_tags2.update({"type" : default_tag_type})
    else:
        pass
    

    return way_tags2

def add_node_type(tag, id, user, lat, lon, uid, version, changeset, timestamp):
    #node values dictionary creating function.
    #using the schmea.py file to get the scheme of the dictionary
    node_attribs= {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'lat': {'required': True, 'type': 'float', 'coerce': float},
            'lon': {'required': True, 'type': 'float', 'coerce': float},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    #updating various values in the dictionary 
    #do I need to do it this way or another way?
    if id:
        node_attribs.update({"id": int (id)})
    if user:
        node_attribs.update({"user": user})
    if lat:
        node_attribs.update({"lat": float(lat)})
    if lon:
        node_attribs.update({"lon": float(lon)})
    if uid:
        node_attribs.update({"uid": int(uid)})
    if version:
        node_attribs.update({"version": version})
    if timestamp:
        node_attribs.update({"timestamp": timestamp})
    if changeset:
        node_attribs.update({"changeset": int(changeset)}) 
    else:
        pass

    return node_attribs 
    #returning the dictionary to the function call.
    

def add_node_tags(tag, id, key, value, default_tag_type):
    #for creating the node tags dictionary
    #using the scheme given above
    tags2 = {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'key': {'required': True, 'type': 'string'},
                'value': {'required': True, 'type': 'string'},
                'type': {'required': True, 'type': 'string'}
            }
    
    if id:
        tags2.update({"id" : int(id)})
    if key:
        tags2.update({"key" : key})
    if value:
        tags2.update({"value": value}) 
        #print "key: ", key, "Value: ", value, "type: ", type(value)
        
    if default_tag_type:
        tags2.update({"type":default_tag_type})
    else:
        pass
    
    return tags2
    #returning tag values to the function call.

    
# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])

         
if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=True)
    print "success! : Look at the csvs in current directory"
    print 'Wrangling, cleaning & writing took', time.time()-start, 'seconds.'
