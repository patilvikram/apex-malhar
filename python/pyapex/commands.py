from pyapex import getApp
import getopt
import sys

def kill_app( app_name ):
  app = getApp( app_name )
  if app :
     print "KILLING APP " + str( app_name )
     app.kill()
  else:
     print "Application not found for name " + str( app_name )

def shutdown_app( app_name ):
  app = getApp( app_name )
  if app :
     print "SHUTTING DOWN APP NOW"
     app.kill()
  else:
     print "Application not found for name " + str( app_name )

func_map = { "KILL_MODE" : kill_app , "SHUTDOWN_MODE" : shutdown_app }
def parse_argument():
    print sys.argv
    try:
        opts, args = getopt.getopt(sys.argv[1:], "k:s:")
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        sys.exit(2)
    for opt, args in opts:
	if opt == '-k':
           func_map['KILL_MODE']( args )
        elif opt == '-s':
           func_map['KILL_MODE']( args )
        else:
	   assert False, "Invalid Option" 

if __name__ == "__main__":
   parse_argument()
