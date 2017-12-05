
#!/usr/bin/env python

import copy, os, pickledb, string, sys, unittest

#####################
#  UNITTEST DRIVER  #
#####################
def unittest_driver() :

  print
  print "*************************************"
  print "*   RUNNING TEST SUITE FOR ONTODS   *"
  print "*************************************"
  print

  os.system( "python -m unittest Test_ontods.Test_ontods.test_example1" )
  os.system( "python -m unittest Test_ontods.Test_ontods.test_example2" )
  os.system( "python -m unittest Test_ontods.Test_ontods.test_example3" )
  os.system( "python -m unittest Test_ontods.Test_ontods.test_example4" )


#########################
#  THREAD OF EXECUTION  #
#########################
unittest_driver()


#########
#  EOF  #
#########
