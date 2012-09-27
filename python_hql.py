#!/usr/bin/python2.7


from multiprocessing import Process,Manager
import os
import sys
import MySQLdb as mdb
import datetime
from optparse import OptionParser


# Default date
_CUR_DATE = datetime.datetime.now().strftime("%Y-%m-%d")

# Implement Args
parser = OptionParser()
parser.add_option("-d","--date",dest="curDate",help="Provide Date",default=_CUR_DATE)

# Get Args
(options,args) = parser.parse_args();

# Assign Args
_CUR_DATE = options.curDate



_HIVE_PATH = "/usr/lib/hive "
_HIVE = "hive "



_CMD_OPT_DAY=_CUR_DATE
_CMD_OPT_PRIORITY='LOW'
_CMD_OPT_TEST_MODE="TRUE"
_CMD_OPT_RUN_STAGE="1"
_CMD_OPT_COMPRESSION_TYPE="RECORD"
_CMD_OPT_VERBOSE="TRUE"


_MYSQL_HOST = "wh1.inapnym.owneriq.net"
_MYSQL_USERNAME = "kbanala"
_MYSQL_PASSWORD = "congress320"
_MYSQL_DATABASE = "WAREHOUSE_HADOOP"

_CMD = {}
_CMD['CREATE_AUCTION'] = """
CREATE EXTERNAL TABLE IF NOT EXISTS auction(
idmostlineitem BIGINT,
sadsize STRING,
scountry STRING,
sregion STRING,
idmacode BIGINT,
sdomainname STRING,
sexchangeuseruuid STRING,
idadnetsegment BIGINT,
isWin BOOLEAN,
isClick BOOLEAN,
isViewConversion BOOLEAN,
isClickConversion BOOLEAN,
utc_time STRING,
cookietimestamp bigint
)
PARTITIONED BY (utc_date STRING)
CLUSTERED BY (idmostlineitem) INTO 128 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/auction/auction';
"""



_CMD['HIVE_SETUP'] = """

SET mapred.job.priority=HIGH;
SET hive.merge.mapfiles=false;
SET hive.exec.parallel=true;
SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET hive.exec.reducers.max=140;
SET mapreduce.reduce.java.opts=-Xmx1g;
SET mapred.job.reuse.jvm.num.tasks=100;
SET mapred.job.name=auction.auction.pivot.%s;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing = true;
SET hive.exec.dynamic.partition=true;


INSERT OVERWRITE TABLE auction PARTITION (utc_date = '%s')
SELECT
idmostlineitem,
sadsize,
scountry,
sregion,
idmacode,
sdomainname,
sexchangeuseruuid,
idadnetsegment,
isWin,
isClick,
isViewConversion,
isClickConversion,
utc_time,
cookietimestamp
FROM
auction_master_pivot
WHERE utc_date='%s';

""" % (_CUR_DATE,_CUR_DATE,_CUR_DATE)



_CMD['MAKE_1'] = """
SELECT
'%s',
sadsize,
scountry,
sregion,
idmacode,
sdomainname,
sexchangeuseruuid
FROM
  auction
WHERE
  utc_date='%s';
""" % (_CUR_DATE,_CUR_DATE)



_CMD['MAKE_2'] = """
SELECT
'%s',
sadsize,
scountry,
sregion,
idmacode,
sdomainname,
sexchangeuseruuid
FROM
  auction
WHERE
  utc_date='%s';
""" % (_CUR_DATE,_CUR_DATE)



_CMD['MAKE_3'] = """
SELECT
'%s',
sadsize,
scountry
FROM
  auction
WHERE
  utc_date='%s';
""" % (_CUR_DATE,_CUR_DATE)


_MAKE_DS = {'MAKE_1':['tblauction','/tmp/tblauction_'+_CUR_DATE+'.tsv'],'MAKE_2':['tblauction1','/tmp/tblauction1_'+_CUR_DATE+'.tsv'],'MAKE_3':['tblauction2','/tmp/tblauction2_'+_CUR_DATE+'.tsv']}


sys.path.append(_HIVE_PATH + "/lib/py");

#_SHARED_STATUS = {}
#_EXIT_STATUS = 1

try:
    con = mdb.connect(_MYSQL_HOST, _MYSQL_USERNAME, _MYSQL_PASSWORD,_MYSQL_DATABASE);
except mdb.Error, e:
    print "MySQL Failed to connect: Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)

def mySQLCom(sql):
    try:
        with con:
            cur = con.cursor()
            cur.execute(sql)
            con.commit()
    except mdb.Error, e:
        return 1 # failed

    return 0 # success


def hiveCom(hql):

    hql_exec = _CMD[hql]
    status = os.system(_HIVE + '  -e "'+hql_exec+'"' )
    if(status != 0):
        print "Intial setup failed"
        sys.exit(1);


MYSQL="/usr/bin/mysql --compress -h wh1.inapnym.owneriq.net -ukbanala -pcongress320 -A WAREHOUSE_HADOOP";

def hiveComP(hql):
    try:
        hql_exec = _CMD[hql]
        table = _MAKE_DS[hql][0]
        output_file = _MAKE_DS[hql][1]

        complete_hive_command = _HIVE + ' -e "' + hql_exec + '" | sed -e "s/NULL/\\N/g" >  ' + output_file
        print complete_hive_command
        os.system(complete_hive_command)
		
		sizeInfo = os.stat(output_file)
		size = sizeInfo.st_size
		
		if(size == 0):
			_SHARED_STATUS[hql] = 1 # failed
			return
		
		


        #sql = MYSQL + " -e 'DELETE FROM "+table+" WHERE dateFact=\""+_CUR_DATE+"\";' "

        mySQLCom("DELETE FROM "+table+" WHERE dateFact=\""+_CUR_DATE+"\"");
        #os.system(sql)
        print "# Deleted FROM "+table+" WHERE dateFact='"+_CUR_DATE+"'"


        #sql = MYSQL + " -e ' ALTER TABLE "+table+" DISABLE KEYS;' "
        #os.system(sql)
        mySQLCom("ALTER TABLE "+table+" DISABLE KEYS");
        #sql =  MYSQL + " -e ' LOAD DATA LOCAL INFILE \""+output_file+"\" INTO TABLE "+table+" FIELDS TERMINATED BY \"\\t\" LINES TERMINATED BY \"\\n\" IGNORE 0 LINES;'"
        #print sql
        sql = "LOAD DATA LOCAL INFILE \""+output_file+"\" INTO TABLE "+table+" FIELDS TERMINATED BY \"\\t\" LINES TERMINATED BY \"\\n\" IGNORE 0 LINES"
        print sql
        #status = os.system(sql)
        if(mySQLCom(sql) == 0):
                _SHARED_STATUS[hql] = 0 # success
        else:
                _SHARED_STATUS[hql] = 1 # failed

        #sql = MYSQL +  " -e ' ALTER TABLE "+table+" ENABLE KEYS;' "
        #os.system(sql)

        mySQLCom("ALTER TABLE "+table+" ENABLE KEYS;");
        os.system("rm "+output_file+"")

    except:
        print '%s' % (sys.exc_info()[0])
        _SHARED_STATUS[hql] = 1


#LOAD DATA LOCAL INFILE '${OUTPUT_FILE}' INTO TABLE ${TBL_NAME} FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 0 LINES
# run set-up HQL
def runSetUp():
    print "runSetUp"
    hiveCom('CREATE_AUCTION')
    hiveCom('HIVE_SETUP')


# run Make HQL in parallel

manager = Manager()
_SHARED_STATUS = manager.dict()

def runMake():
    print "Run Make"


    make1_proc = Process(target=hiveComP, args=('MAKE_1',))
    make1_proc.start()

    make2_proc = Process(target=hiveComP, args=('MAKE_2',))
    make2_proc.start()

    make3_proc = Process(target=hiveComP, args=('MAKE_3',))
    make3_proc.start()

    make1_proc.join();
    make2_proc.join();
    make3_proc.join();


# send result
def sendSignal():
    # if any of make HQL is failed, return 1 ( failed )
    print _SHARED_STATUS
    if(_SHARED_STATUS['MAKE_1'] != 0 or _SHARED_STATUS['MAKE_2'] != 0 or _SHARED_STATUS['MAKE_3'] != 0):
        print 2
        sys.exit(2)
    else:
       # if all HQL is worked great, return 0 ( success )
        print 0
        sys.exit(0)



# entry point
if __name__ == '__main__':
    runSetUp()
    runMake()
    sendSignal()
