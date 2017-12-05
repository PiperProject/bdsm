
#!/usr/bin/env python

'''
Test_ontods.py
'''


#############
#  IMPORTS  #
#############
# standard python packages
import inspect, logging, os, pickledb, pprint, random, rdflib, sqlite3, sys, unittest
from StringIO import StringIO
from pymongo import MongoClient

import OntoDS

SAVEPATH      = os.path.abspath( __file__ + "/../../../ontods/src" )

CURR_PATH     = os.path.abspath( __file__ + "/..")
MONGOSAVEPATH = os.path.abspath( __file__ + "/../../dbtmp")


#################
#  TEST ONTODS  #
#################
class Test_ontods( unittest.TestCase ) :

  #logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.DEBUG )
  logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.INFO )


  ###############
  #  EXAMPLE 4  #
  ###############
  # test passing verify on insert for mongo db
  def test_example4( self ) :

    test_id = "test_example4"

    logging.info( "  Running test " + test_id )

    # --------------------------------------------------------------- #
    # create ontods instance
    ontods = OntoDS.OntoDS( "mongodb" )
    ontods.MONGOSAVEPATH = MONGOSAVEPATH
    logging.debug( "  " + test_id + " : instantiated OntoDS instance '" + str( ontods ) + "' with db type '" + ontods.nosql_type + "'"  )

    # --------------------------------------------------------------- #
    # input ontology

    ontods.loadOntology( "./example_ontology.ttl" )

    # --------------------------------------------------------------- #
    # build database

    anInsert = { "name":"Elsa", "age":21, "City":"arendelle", "Country":"norway" }
    self.assertEqual( ontods.verify( anInsert, [ 'name', 'age' ] ), True )

    # --------------------------------------------------------------- #
    if ontods.verify( anInsert, [ 'name', 'age' ] ) :

      # --------------------------------------------------------------- #
      # create db instance
  
      self.createMongoInstance( MONGOSAVEPATH )
      client = MongoClient()
      db = client.testdb
  
      # --------------------------------------------------------------- #
      # perform insertion

      id1 = db.testdb.insert_one( anInsert ).inserted_id
  
      # --------------------------------------------------------------- #
      # output db contents

      logging.debug( "results : " + str( db.testdb.find_one( { "_id": id1 } ) ) )
      contents = db.testdb.find_one( { "_id": id1 } )
      thing    = contents.pop( "_id", None )
      logging.debug( "contents : " + str( contents ) )

      self.assertEqual( contents, {u'City': u'arendelle', u'Country': u'norway', u'age': 21, u'name': u'Elsa'} )
  
      # --------------------------------------------------------------- #
      # destroy instance
      db.testdb.drop()
  
      client.close()
  
      # get instance id
      os.system( "pgrep mongod 2>&1 | tee dbid.txt" )
      fo     = open( "dbid.txt", "r" )
      dbid   = fo.readline()
      fo.close()
      os.system( "rm " + CURR_PATH + "/dbid.txt" )
      os.system( "kill " + dbid )

    # --------------------------------------------------------------- #


  ###############
  #  EXAMPLE 3  #
  ###############
  # test passing verify on insert for pickle db
  def test_example3( self ) :

    test_id = "test_example3"

    logging.info( "  Running test " + test_id )

    # --------------------------------------------------------------- #
    # create ontods instance
    ontods = OntoDS.OntoDS( "pickledb" )
    logging.debug( "  " + test_id + " : instantiated OntoDS instance '" + str( ontods ) + "' with db type '" + ontods.nosql_type + "'"  )

    # --------------------------------------------------------------- #
    # input ontology

    ontods.loadOntology( "./example_ontology.ttl" )

    # --------------------------------------------------------------- #
    # build database

    anInsert = { "name":"Elsa", "age":21, "City":"arendelle", "Country":"norway" }

    self.assertEqual( ontods.verify( anInsert, [ 'name', 'age' ] ), True )

    # --------------------------------------------------------------- #
    if ontods.verify( anInsert, [ 'name', 'age' ] ) :

      # --------------------------------------------------------------- #
      # create db instance

      logging.info( "  " + test_id + ": initializing pickledb instance." )
      dbInst = pickledb.load( "./test_yprov.db", False )

      # --------------------------------------------------------------- #
      # perform insertion

      anID = "thisIsAnID"
      dbInst.set( anID, anInsert )

      # --------------------------------------------------------------- #
      # output db contents

      logging.debug( "OUTPUTTING PICKLE DB CONTENTS TO STDOUT :" )
      allKeys = dbInst.getall()
      for key in allKeys :
        logging.debug( dbInst.get( key ) )

      self.assertEqual( dbInst.get( anID ), {'City': 'arendelle', 'age': 21, 'name': 'Elsa', 'Country': 'norway'} )

      # --------------------------------------------------------------- #
      dbInst.deldb()

    # --------------------------------------------------------------- #


  ###############
  #  EXAMPLE 2  #
  ###############
  # test failing verify and explanation
  def test_example2( self ) :

    test_id = "test_example2"

    logging.info( "  Running test " + test_id )

    # --------------------------------------------------------------- #
    # create ontods instance
    ontods = OntoDS.OntoDS( "pickledb" )
    logging.debug( "  " + test_id + " : instantiated OntoDS instance '" + str( ontods ) + "' with db type '" + ontods.nosql_type + "'"  )

    # --------------------------------------------------------------- #
    # input ontology

    ontods.loadOntology( "./example_ontology.ttl" )

    # --------------------------------------------------------------- #
    # build database

    anInsert = { "name":"Elsa", "age":21, "City":"losangeles", "Country":"norway" } 

    self.assertEqual( ontods.verify( anInsert, [ 'name', 'age' ] ), False )
    self.assertEqual( ontods.explain( anInsert, [ 'name', 'age' ] ), ["EXPLANATION : no predicates map subject 'losangeles' to object 'City'"] )

    # --------------------------------------------------------------- #


  ###############
  #  EXAMPLE 1  #
  ###############
  # test bad load file
  def test_example1( self ) :

    test_id = "test_example1"

    logging.info( "  Running test " + test_id )

    # --------------------------------------------------------------- #
    # create ontods instance
    ontods = OntoDS.OntoDS( "pickledb" )
    logging.debug( "  " + test_id + " : instantiated OntoDS instance '" + str( ontods ) + "' with db type '" + ontods.nosql_type + "'"  )

    # --------------------------------------------------------------- #
    # input ontology

    with self.assertRaises(SystemExit) as cm:
      exitResult = ontods.loadOntology( "./someFileName.ttl" )
    self.assertEqual( cm.exception.code, "  LOAD ONTOLOGY : file not found './someFileName.ttl'" )

    # --------------------------------------------------------------- #


  # =========================================================================== #
  # =========================================================================== #

  ###########################
  #  CREATE MONGO INSTANCE  #
  ###########################
  def createMongoInstance( self, DBPATH ) :
  
    logging.debug( "Creating mongo db instance at " + DBPATH + "\n\n" )
  
    # establsih clean target dir for db
    if not os.path.exists( DBPATH ) :
      os.system( "mkdir " + DBPATH + " ; " )
    else :
      os.system( "rm -rf " + DBPATH + " ; " )
      os.system( "mkdir " + DBPATH + " ; " )
  
    # build mongodb instance
    os.system( "mongod --dbpath " + DBPATH + " &" )


#########
#  EOF  #
#########
