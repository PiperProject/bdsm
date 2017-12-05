#!/usr/bin/env python

##########################################################################
# OntoDS usage notes:
#
# 1. Currently limited to insertions.
# 2. Only supports subsumption verification on maps of strings to strings.
# 3. Only supports subsumption verification using RDF ontologies.
#
##########################################################################

# -------------------------------------- #
import logging, os, pprint, pydot, rdflib, string, sys, time

# import sibling packages HERE!!!

# adapters path
adaptersPath  = os.path.abspath( __file__ + "/../../../../adapters" )
if not adaptersPath in sys.path :
  sys.path.append( adaptersPath )
import Adapter

# settings dir
settingsPath  = os.path.abspath( __file__ + "/../../core" )
if not settingsPath in sys.path :
  sys.path.append( settingsPath )
import settings

# -------------------------------------- #


DEBUG = settings.DEBUG


class OntoDS( object ) :


  ################
  #  ATTRIBUTES  #
  ################
  nosql_type    = None   # the type of nosql database under consideration
  ontology      = None   # an rdflib Graph object instance
  MONGOSAVEPATH = None


  ##########
  #  INIT  #
  ##########
  def __init__( self, nosql_type ) :

    # save nosql db type
    self.nosql_type = nosql_type

    # instantiate the ontology graph
    self.ontology  = rdflib.Graph()

    logging.debug( "  ...instantiated OntoDS instance with ontology object '" + str( self.ontology ) + "'" )


  ###################
  #  LOAD ONTOLOGY  #
  ###################
  # load the ontology at the given file path
  def loadOntology( self, ontoPath ) :

    if os.path.isfile( ontoPath ) :

      logging.debug( "  LOAD ONTOLOGY : loading ontology from '" + ontoPath + "'" )

      self.ontology.parse( ontoPath, format="nt" )
      for stmt in self.ontology :
        pprint.pprint(stmt)

    else :
      sys.exit( "  LOAD ONTOLOGY : file not found '" + ontoPath + "'" )


  ################
  #  ADD TRIPLE  #
  ################
  # input the subject, predicate, and object of a new 
  # triple to add to the ontology.
  def addTriple( self, subj, pred, obj ) :
    self.ontology.add( ( subj, pred, obj) )


  ####################
  #  PRINT ONTOLOGY  #
  ####################
  def printOntology( self ) :
    for stmt in self.ontology :
      pprint.pprint(stmt)


  ############
  #  VERIFY  #
  ############
  # given an insert/update query, determine if the semantics
  # align with the semantics in the given ontology.
  # assume insert styled in key-value format.
  # return true on satisfaction
  # return false on dissatisfaction
  # also input a list of keys to ignore.
  def verify( self, queryMap, ignoreList ) :

    # get all data in query st keys are subjects in the ontology
    for k in queryMap :

      if not k in ignoreList :

        v = queryMap[ k ]

        # make sure KV pairs obey ontology subsumption rules
        if not self.passesKVSubsumption( k, v ) :
          logging.debug( "  VERIFY : query fails on KV subsumption for key '" + str( k ) + "' and val '" + v + "'" )
          return False

        # make sure values across keys obey ontology subsumption rules
        elif not self.passesMultiKeySubsumption( k, v, queryMap ) : 
          logging.debug( "  VERIFY : query fails on Multi Key subsumption for key '" + str( k ) + "' and val '" + v + "'" )
          return False

    return True


  #############
  #  EXPLAIN  #
  #############
  # given an insert/update query, explain why the semantics
  # align with the semantics of the given ontology.
  # also input a list of keys to ignore.
  def explain( self, queryMap, ignoreList ) :

    explanations = []

    # get all data in query st keys are subjects in the ontology
    for k in queryMap :

      if not k in ignoreList :

        v = queryMap[ k ]

        # make sure KV pairs obey ontology subsumption rules
        if not self.passesKVSubsumption( k, v ) :
          logging.debug( "  EXPLAIN : fails KV Subsumption : key '" + k +  "', value '" + v + "'" )
          explanations.append( self.explainKVSubsumption( k, v ) )

        # make sure values across keys obey ontology subsumption rules
        elif not self.passesMultiKeySubsumption( k, v, queryMap ) :
          logging.debug( "  EXPLAIN : fails Multi Key Subsumption : key '" + k +  "', value '" + v + "'" )
          explanations.append( self.explainMultiKeySubsumption( k, v, queryMap ) )

    return explanations


  ###################################
  #  EXPLAIN MULTI KEY SUBSUMPTION  #
  ###################################
  # provides explanations for both working and failing insertions/updates
  def explainMultiKeySubsumption( self, k, v, queryMap ) :

    logging.debug( "  EXPLAIN MULTI KEY SUBSUMPTION : running test..." )

    # CASE : PICKLE DB
    if self.nosql_type == "pickledb" :
      return self.explainMultiKeySubsumption_pickledb( k, v, queryMap )

    # CASE : MONGO DB
    elif self.nosql_type == "mongodb" :
      return self.explainMultiKeySubsumption_pickledb( k, v, queryMap )

    # WTF???
    else :
      sys.exit( "  EXPLAIN MULTI KEY SUBSUMPTION : ERROR : unrecognized nosql_typ '" + str( self.nosql_type ) + "'" )


  ############################################
  #  EXPLAIN MUTI KEY SUBSUMPTION PICKLE DB  #
  ############################################
  def explainMultiKeySubsumption_pickledb( self, key1, val1, queryMap ) :

    logging.debug( "  EXPLAIN MULTI KEY SUBSUMPTION PICKLE DB : running test..." )

    for key2 in queryMap :

      val2 = queryMap[ key2 ]

      #print "key1 = " + key1 + ", key2 = " + key2
      if self.checkContainment( key1, key2 ) :

        # grab the predicates relating keys 1 and 2
        # assume the same predicates relate the corresponding data in the ontology.
        predList = self.getPredicates( key1, key2 )
        #print predList 

        if not self.checkContainment( val1, val2 ) :
          return "EXPLANATION : no predicates map subject '" + str( val1 ) + "' to object '" + str( key1 ) + "'"

    return "EXPLANATION : subject '" + str( val1 ) + "' maps to object '" + str( key2 ) + "' adheres to all relevant predicates."


  ###########################################
  #  EXPLAIN MUTI KEY SUBSUMPTION MONGO DB  #
  ###########################################
  def explainMultiKeySubsumption_mongodb( self, key, val, queryMap ) :
    return None


  ############################
  #  EXPLAIN KV SUBSUMPTION  #
  ############################
  def explainKVSubsumption( self, key, val ) :

    logging.debug( "  EXPLAIN KV SUBSUMPTION : running test..." )

    subjs = self.getSubjects( val )
    objs  = self.getObjects( key )

    if len( subjs ) < 1 or len( objs ) < 1 :
      return "EXPLANATION : no predicates map subject '" + str( val ) + "' to object '" + str( key ) + "'"

    predList = []
    for s in subjs :
      flag = False

      # make sure every subject is subsumed by some valid object
      for o in objs :
        if ( s, None, o ) in self.ontology :
          newPreds = self.getPredicates( s, o )
          for p in newPreds :
            if not p in predList :
              predList.append( p )
          flag = True

      # return False otherwise
      if not flag :
        return "EXPLANATION : no predicates map subject '" + str( val ) + "' to object '" + str( key ) + "'"

    return "EXPLANATION : subject '" + str( val ) + "' maps to object '" + str( key ) + "' via predicates : " + str( predList )


  ###########################
  #  PASSES KV SUBSUMPTION  #
  ###########################
  # make sure keys subsume values according to the ontology.
  def passesKVSubsumption( self, key, val ) :

    logging.debug( "  PASSES KV SUBSUMPTION : running test..." )

    subjs = self.getSubjects( val )
    objs  = self.getObjects( key )

    for s in subjs :
      flag = False

      # make sure every subject is subsumed by some valid object
      for o in objs :
        if ( s, None, o ) in self.ontology :
          flag = True

      # return False otherwise
      if not flag :
        return False

    return True


  ##################################
  #  PASSES MULTI KEY SUBSUMPTION  #
  ##################################
  # make sure data across related keys pass subsumption rules.
  def passesMultiKeySubsumption( self, key, val, queryMap ) :

    logging.debug( "  PASSES MULTI KEY SUBSUMPTION : running test..." )

    # CASE : PICKLE DB
    if self.nosql_type == "pickledb" :
      return self.passesMultiKeySubsumption_pickledb( key, val, queryMap )

    # CASE : MONGO DB
    elif self.nosql_type == "mongodb" :
      return self.passesMultiKeySubsumption_pickledb( key, val, queryMap )

    # WTF???
    else :
      sys.exit( "  PASSES MULTI KEY SUBSUMPTION : ERROR : unrecognized nosql_typ '" + str( self.nosql_type ) + "'" )


  ###########################################
  #  PASSES MUTI KEY SUBSUMPTION PICKLE DB  #
  ###########################################
  def passesMultiKeySubsumption_pickledb( self, key1, val1, queryMap ) :

    logging.debug( "  PASSES MULTI KEY SUBSUMPTION PICKLE DB : running test..." )

    for key2 in queryMap :

      val2 = queryMap[ key2 ]

      #print "key1 = " + key1 + ", key2 = " + key2
      if self.checkContainment( key1, key2 ) :

        # grab the predicates relating keys 1 and 2
        # assume the same predicates relate the corresponding data in the ontology.
        predList = self.getPredicates( key1, key2 )
        #print predList 

        if not self.checkContainment( val1, val2 ) :
          logging.debug( "  PASSES MULTI KEY SUBSUMPTION PICKLE DB : containment failed for val1 '" + str( val1 ) + "' and val2 '" + val2 + "'" )
          return False

    return True


  ##########################################
  #  PASSES MUTI KEY SUBSUMPTION MONGO DB  #
  ##########################################
  def passesMultiKeySubsumption_mongodb( self, key, val, queryMap ) :
    return None


  ####################
  #  GET PREDICATES  #
  ####################
  # grab the list of predicates in the ontology connecting 
  # the given subject and object keys
  def getPredicates( self, key_subj, key_obj ) :

    logging.debug( "------------------------------------------" )
    logging.debug( "  GET PREDICATES : running process..." )
    logging.debug( "    key_subj = " + str( key_subj ) )
    logging.debug( "    key_obj  = " + str( key_obj ) )

    # get all subject/object combos satisfying the subject and object keys
    predList = []

    for (s,p,o) in self.ontology :

      logging.debug( "  GET PREDICATES : (s,p,o) = " + str( ( s,p,o ) ) )
      logging.debug( "  GET PREDICATES : s = " + str( s ) )
      logging.debug( "  GET PREDICATES : o = " + str( o ) )
      logging.debug( "  GET PREDICATES : key_subj == s is " + str( key_subj == s ) )
      logging.debug( "  GET PREDICATES : key_obj == o is " + str( key_obj == o ) )
      logging.debug( "  GET PREDICATES : key_subj in s is " + str( key_subj in s ) )
      logging.debug( "  GET PREDICATES : key_obj in o is " + str( key_obj in o ) )


      #cleanSubj = self.parseData( s )
      #cleanObj  = self.parseData( o )

      #print "Comparing key_subj = " + str( key_subj ) + " and cleanSubj = " + cleanSubj
      #if cleanSubj.lower() == str( key_subj ) or cleanSubj == str( key_subj ) :
      #if key_subj == s :
      if key_subj in s :

        #print "Comparing key_obj = " + str( key_obj ) + " and cleanObj = " + cleanObj
        #if cleanObj.lower() == str( key_obj ) or cleanObj == str( key_obj ) :
        #if key_obj == o :
        if key_obj in o :
          #print " p = " + str( p )
          logging.debug( "  GET PREDICATES : >>> adding predicate '" + str( p ) + "'" )
          predList.append( p )


    # abort if no valid predicates in ontology
    if len( predList ) < 1 :
      sys.exit( "  GET PREDICATES : ERROR : no predicates relating key_subj '" + str( key_subj ) + "' and key_obj '" + str( key_obj ) + "'" )

    logging.debug( "  GET PREDICATES : returning predList = " + str( predList ) )
    logging.debug( "------------------------------------------" )
    return predList


  #######################
  #  CHECK CONTAINMENT  #
  #######################
  # checks only direct subsumption rules
  # e.g. if city < state and state < country, then will conclude city < country is false.
  #      if the ontology additionally defines city < country, then will conclude true.
  # TODO : recursion!!!
  def checkContainment( self, key_subj, key_obj ) :

    logging.debug( "  CHECK CONTAINMENT : running process..." )
    logging.debug( "  CHECK CONTAINMENT : key_subj = " + str( key_subj ))
    logging.debug( "  CHECK CONTAINMENT : key_obj  = " + str( key_obj ) )

    for (s,p,o) in self.ontology :

      #cleanSubj = self.parseData( s )
      #cleanObj  = self.parseData( o )

      #print "Comparing key_subj = " + key_subj + " and cleanSubj = " + cleanSubj
      #if cleanSubj.lower() == key_subj or cleanSubj == key_subj :
      if key_subj in s :

        #print "Comparing key_obj = " + key_obj + " and cleanObj = " + cleanObj
        #if cleanObj.lower() == key_obj or cleanObj == key_obj :
        if key_obj in o :
          return True

    return False


  ##################
  #  GET SUBJECTS  #
  ##################
  # get all subject strs matching the input value string
  def getSubjects( self, val ) :

    allSubjects = []

    for s in self.ontology.subjects( ) : 

      orig_s = s

      # grab subject from uri
      s = self.parseData( s )

      if val == s :
        allSubjects.append( orig_s )

      elif val == s.lower() :
        allSubjects.append( orig_s )

    return allSubjects


  #################
  #  GET OBJECTS  #
  #################
  # get all object strs matching the input value string
  def getObjects( self, val ) :

    allObjects = []

    for o in self.ontology.objects( ) : 

      orig_o = o
      obj = self.parseData( o )

      if val == obj :
        allObjects.append( orig_o )

      elif val == obj.lower() :
        allObjects.append( orig_o )

    return allObjects


  ################
  #  PARSE DATA  #
  ################
  # parse the data string from the given uri
  def parseData( self, uri ) :

    str_uri = uri.n3()
    str_uri = str_uri.encode( 'utf-8' )
    str_uri = str_uri.replace( ">", "" )
    str_uri = str_uri.replace( "<", "" )

    #print "uri = " + uri,
    #print "type = " + str( type( uri ) )
    #print "str_uri = " + str_uri,
    #print "type = ",
    #print type( str_uri )

    # parse literals
    if "'" in str_uri or '"' in str_uri :
      str_uri = str_uri.replace( "'", "" )
      str_uri = str_uri.replace( '"', "" )
      return str_uri

    # parse foaf
    elif "foaf" in str_uri :
      data = str_uri.split( "/" )
      data = data[-1]
      return data

    # parse schema.org
    elif "schema.org" in str_uri :
      data = str_uri.split( "/" )
      data = data[-1]
      return data

    # parse example.org
    elif "example.org" in str_uri :
      data = str_uri.split( "/" )
      data = data[-1]
      return data

    else :
      sys.exit( "  PARSE DATA : ERROR : unrecognized data type '" + str( type( str_uri ) ) + "' for string uri '" + str( str_uri ) + "'" )


#########
#  EOF  #
#########
