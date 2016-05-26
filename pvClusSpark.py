import cred
import pymssql
import time
import math
import igraph
from igraph import *
import csv
import numpy as np
from colour import Color
import itertools
import pandas
import numpy
import json
import collections
from compiler.ast import flatten

from pyspark import SparkContext,SparkConf
import time
import math


"""
Class for pageview clustering using different community detection
algorithms on SKU network defined by point-wise mutual information

:param
    startDate ('2016-01-01' format): pageviews after this collected
    endDate ('2016-01-01' format): pageviews before this collected
    catLevel: categorization level (b.SubCategory or b.SubSubCategory)
    sscat: value for catLevel (e.g. 'Wood Flooring', 'Laminate Flooring')
    timeThresh (default 5): minimum time in seconds to spend on a page to be included
    minViewThresh (default 5): minimum (exclusive) of pageview for a SKU to be in analysis
    jointViewThresh (default 1): minimum (exclusive) joint pageviews for two SKUs to be in analysis
    norm: PMI normalization indices:
        1: normalized PMI confined to [-100, 100] range (default) [preferred since 0 means independence]
        2: powered PMI
        3: PMI
        4: PMI^2
        5: PMI^3
    method: Community detection indices:
        1: fast greedy
        2: walk trap
        3: leading eigenvector
        4: multilevel (Louvain) (default)
        5: edge betweenness - slower, one big cluster
        6: label propagation
        7: spinglass (the only one that takes negative edge weights)
        8: spinglass with positive edge weights
    k: number of clusters (optional - not applicable to all methods)
    sg_gamma (Applicable only if method = 7, has to be > 0 - default 1): spin glass community detection parameter -
            specifying the balance between the
            importance of present and missing edges within a community. The default value of 1.0 assigns equal
            importance to both of them.. "From paper:Increasing gamma+ raises the threshold for nodes to be clustered,
            and will (generally) result in smaller communities detected, possibly embedded in larger and sparser
            communities
    sg_lambda (Applicable only if method = 7 - default 1): spin glass community detection parameter - specifies the
            balance between the importance of
            present and missing negatively weighted edges within a community. Smaller values of lambda lead to
            communities with less negative intra-connectivity. If the argument is zero, the algorithm reduces to a
            graph coloring algorithm, using the number of spins as colors. Increasing gamma- (lambda) has the opposite
            effect and lowers the threshold, and will (generally) result in a configuration of larger communities
    clSizeThresh (optional - default = 40): the threshold for the size of cluster - larger than this will be broken
            into smaller ones
    subMethod (optional - default = 8): the commmunity detection method for breaking the large cluster specified by
            clSizeThresh
    iterCount: number of times to try to break the clusters by adjusting the parameters of Comm Detect Alg (gamma in
            increased by 0.5 increments for spin glass comm detection in the current version)
Created: Sina G, March 2016
Updates: Sina G, April 2016, May 2016
"""

class PVCluster(object):

    def __init__(self,
                 startDate,
                 endDate,
                 catLevel,
                 sscat,
                 timeThresh=5,
                 minViewThresh=5,
                 jointViewThresh=1,
                 pmiPerc=5,
                 norm=1,
                 method=4,
                 k=None,
                 sg_gamma=1,
                 sg_lambda=1,
                 clSizeThresh=40,
                 subMethod=8,
                 iterCount=3):

        self.startDate = startDate
        self.endDate = endDate
        self.catLevel = catLevel
        self.sscat = sscat
        self.timeThresh = timeThresh
        self.minViewThresh = minViewThresh
        self.jointViewThresh = jointViewThresh
        self.pmiPerc = pmiPerc
        self.norm = norm
        self.method = method
        self.k = k
        self.sg_gamma = sg_gamma
        self.sg_lambda = sg_lambda
        self.clSizeThresh = clSizeThresh
        self.subMethod = subMethod
        self.iterCount = iterCount

        self.ODBC_PROD = 'DBSQLSVR01'
        self.ODBC_DEV = 'V1DEDB01'
        self.db = 'DW0'

    def readData(self):
        """
        Read pageview data from the database
        creates the potential pageviews for each SKU and the pairs with common views
        the process is coded in Spark
        the rest of the modules are done in Python
        :return: writes a CSV File with these fields:
            sku1,
            sku2,
            pv for sku1,
            pv for sku2,
            common PV for sku1 and sku2,
            totalPV in the whole dataset (not used for current analysis),
            potential PV for SKU1 (based on subsubcat and visible date),
            potential PV for SKU2 (based on subsubcat and visible date),
            potential joint PV for SKU1 and SKU2 (based on subsubcat and overlapping visible date)
        """

        # WE NEED TO FIX THE READS FROM THE DATABASE:
        # EXAMPLE CODE FOR SPARK READING FROM DATABASE:
            df_productMetrics = sqlContext.\
    #        read.format('jdbc').\
    #        options(url="""jdbc:jtds:sqlserver://10.0.2.85;domain=BUILDDIRECT;
    #        appName=RazorSQL;useCursors=true;user="""+cred.user+""";password=""" + cred.passw+""";""",
    #                dbtable=
    #                 """
    #                        (select productItemID,
    #                                numRatedCarts,
    #                                numOrders,
    #                                subSubCategory,
    #                                conv_crt_to_ord,
    #                                /*conv_crt_to_ord,
    #                                conv_crt_to_ord_Georgia,
    #                                conv_crt_to_ord_Illinois,
    #                                conv_crt_to_ord_California,
    #                                conv_crt_to_ord_NewJersey,
    #                                conv_crt_to_ord_Texas,*/
    #                                CEILING((PERCENT_RANK()OVER(PARTITION BY subSubCategory ORDER BY conv_crt_to_ord ASC))*100)
    #                                as percentile_cart_to_ord,
    #                                CEILING((PERCENT_RANK()OVER(PARTITION BY subSubCategory ORDER BY conv_crt_to_ord_Georgia ASC))*100)
    #                                as percentile_cart_to_ord_Georgia,
    #                                CEILING((PERCENT_RANK()OVER(PARTITION BY subSubCategory ORDER BY conv_crt_to_ord_Illinois ASC))*100)
    #                                as percentile_cart_to_ord_Illinois,
    #                                CEILING((PERCENT_RANK()OVER(PARTITION BY subSubCategory ORDER BY conv_crt_to_ord_California ASC))*100)
    #                                as percentile_cart_to_ord_California,
    #                                CEILING((PERCENT_RANK()OVER(PARTITION BY subSubCategory ORDER BY conv_crt_to_ord_NewJersey ASC))*100)
    #                                as percentile_cart_to_ord_NewJersey,
    #                                CEILING((PERCENT_RANK()OVER(PARTITION BY subSubCategory ORDER BY conv_crt_to_ord_Texas ASC))*100)
    #                                as percentile_cart_to_ord_Texas
    #                                from dw0..vw_ET_ProductMetrics) as A
    #                 """,
    #                driver = "net.sourceforge.jtds.jdbc.Driver").load().cache()
    # df_productMetrics.registerTempTable('T_Convo')



        start_time = time.time()
        # read SKU number and the VisitorID
        # conn = pymssql.connect(server=self.ODBC_PROD,
        #                         database=self.db,
        #                         user=cred.user,
        #                         password=cred.pw)
        # readerFunc = conn.cursor(as_dict=False)
        # readerFunc.execute("""
        #                     SELECT a.[ProductItemID]
        #                           ,a.[VisitorId]
        #                     FROM DW0.dbo.AdobePageViewBySession a
        #                     		INNER JOIN DM0.dbo.Products b
			#                             on a.ProductItemID = b.ProductItemID
        #                     WHERE a.TimeSpentOnPage > %d
        #                         and a.VisitDate > '%s'
        #                         and a.VisitDate < '%s'
        #                         and b.VisibleOnWeb = 1
        #                         and %s = '%s'
        #                     ORDER BY a.[ProductItemID]
        #                     """  %(self.timeThresh,
        #                            self.startDate,
        #                            self.endDate,
        #                            self.catLevel,
        #                            self.sscat))

        # Read files from text - to be replaced with SQL read like above
        pvData = []
        with open('skuVisits.txt', 'r') as inputfile:
            for line in inputfile:
                pvData.append(line.strip().replace('\t',',').split(','))



        readerFunc.execute("""
                            SELECT count (distinct a.visitorID)
                            FROM DW0.dbo.AdobePageViewBySession a
                            		INNER JOIN DM0.dbo.Products b
			                        on a.ProductItemID = b.ProductItemID
                            WHERE a.TimeSpentOnPage > %d
                                and a.VisitDate > '%s'
                                and a.VisitDate < '%s'
                                and b.VisibleOnWeb = 1
                                and %s = '%s'
                            """ %(self.timeThresh, self.startDate, self.endDate, self.catLevel, self.sscat))

        totalVisits = readerFunc.fetchall()
        totalVisits = totalVisits[0][0]
        print "Total Visits Count: ", totalVisits

        print("--- %s seconds --- Read from the database" % (time.time() - start_time))
        conn.close()



        # read SKU number and the VisitorID
        # conn = pymssql.connect(server = self.ODBC_PROD,
        #                         database = self.db,
        #                         user = cred.user,
        #                         password = cred.pw)
        # readerFunc = conn.cursor(as_dict = False)
        # readerFunc.execute("""
        #                     SELECT a.[ProductItemID],
        #                          max(b.FirstVisibleDate) FirstVisibleDate,
        #                         --	b.Category,
        #                         --  min(b.SubCategory)
        #                         	min(b.SubSubCategory)
        #                     FROM DW0..[AdobePageViewBySession] a
        #                             INNER JOIN DM0..Products b
        #                                 on a.ProductItemID = b.ProductItemID
        #                     WHERE a.TimeSpentOnPage > %d
        #                             and a.VisitDate > '%s'
        #                             and a.VisitDate < '%s'
        #                             and b.VisibleOnWeb = 1
        #                             and %s = '%s'
        #                     GROUP BY a.ProductItemID
        #                     ORDER BY a.ProductItemID
        #                    """ %(self.timeThresh,
        #                          self.startDate,
        #                          self.endDate,
        #                          self.catLevel,
        #                          self.sscat))
        # skuVisible = readerFunc.fetchall()

        # THIS SHOULD BE REPLACES WITH A SQL similar to the one in top
        skuVisible = []
        with open('skuVisible.txt', 'r') as inputfile:
            for line in inputfile:
                # print line
                skuVisible.append(line.strip().replace('\t',',').split(','))


        # readerFunc.execute("""
        #                     SELECT count(distinct  a.[UserId]) as VisitCount,
        #                               convert (date, a.VisitDate) as VisitDate,
        #                             --	b.Category,
        #                             --  b.SubCategory
        #                             	b.SubSubCategory
        #                     FROM DW0..[AdobePageViewBySession] a
        #                         INNER JOIN DM0..Products b
        #                             on a.ProductItemID = b.ProductItemID
        #                     WHERE a.TimeSpentOnPage > %d
        #                         and a.VisitDate > '%s'
        #                         and a.VisitDate < '%s'
        #                         and b.VisibleOnWeb = 1
        #                         and %s = '%s'
        #                     GROUP BY b.SubSubCategory, convert (date, a.VisitDate)
        #                     ORDER BY b.SubSubCategory, VisitDate
        #                     """ %(self.timeThresh,
        #                           self.startDate,
        #                           self.endDate,
        #                           self.catLevel,
        #                           self.sscat))
        # # Generating dictionary with SKU as key and all VisitorIds (sessions) as a set
        # dateSubcat = readerFunc.fetchall()


        # THIS SHOULD BE REPLACES WITH A SQL similar to the one in top
        dateSubcat = []
        with open('dateSubcat.txt', 'r') as inputfile:
            for line in inputfile:
                dateSubcat.append(line.strip().replace('\t',',').split(','))


        # Parallelize collections, creating RDD
        distData = sc.parallelize(pvData)
        skuVisibleRDD = sc.parallelize(skuVisible)
        dateSubCatRDD = sc.parallelize(dateSubcat)

        # transforming the visit ID to set. unionized the visits for each sku number which serves as a key
        skuViewsSet = distData.map(lambda (k,v):(k,set([v]))).reduceByKey(lambda v1,v2:v1.union(v2))


        # Filtering the skus with less than a threshold (hardcoded in this version) visits
        skuViewsSet = skuViewsSet.filter(lambda (sku,visits): len(visits)>self.minViewThresh)


        # creating the vector to calculate PMI for each pair (with similar denominators)
        skuPairViews = skuViewsSet.cartesian(skuViewsSet).filter(lambda (s1,s2):s1[0]!=s2[0])


        # create the sku pair, visit counts, shared visits counts, total pageview
        # filter for pairs with some shared view
        skuPairViewsFilt = skuPairViews.map(lambda (s1,s2):list( [s1[0],
                                                                  s2[0],
                                                                  len(s1[1]),
                                                                  len(s2[1]),
                                                                  len(s1[1].intersection(s2[1])),
                                                                  1000])).filter(lambda s:s[4]>self.jointViewThresh)

        # Creating the denominator numbers and recording all of them as vector
        # Calculating the denominator for PMI for each SKU and each Pair
        # Mimicing the createDenomData(self) module in the code

        from pyspark.sql import SQLContext
        sqlContext = SQLContext(sc)

        # repeating this 2 lines - do not need it in the full code
        sqlContext.createDataFrame(skuVisibleRDD, ['ProductItemID',
                                                   'FirstVisibleDate',
                                                   'SubSubCategory']).registerTempTable('skuVisibleDF')
        sqlContext.createDataFrame(dateSubCatRDD, ['VisitCount',
                                                   'VisitDate',
                                                   'SubSubCategory']).registerTempTable('dateSubCatDF')

        sqlContext.createDataFrame(skuPairViewsFilt, ['sku1',
                                                      'sku2',
                                                      'pv1',
                                                      'pv2',
                                                      'comPV',
                                                      'totalPV']).registerTempTable('skuPairViewsFiltDF')


        # Generating the aggregate number of PV for each SKU based on its SSCat and visible date
        sqlContext.sql(
            """
             SELECT ProductItemID,
                     sum(VisitCount) as AggVisitCount,
                     min(SubSubCategory) as SubSubCategory,
                     min(FirstVisibleDate) as FirstVisibleDate
             FROM (
                     SELECT a.ProductItemID
                            ,cast(a.FirstVisibleDate as date)
                            ,cast(b.VisitCount as int)
                            ,cast(b.VisitDate as date)
                            ,b.SubSubCategory
                     FROM skuVisibleDF a
                         INNER JOIN dateSubCatDF b
                         on a.SubSubCategory = b.SubSubCategory
                             and  b.VisitDate >= a.FirstVisibleDate
                  ) c
             GROUP BY c.ProductItemID
            """).cache().registerTempTable('aggPVcount')


        # Generating maximum visit date and subsubcat info for SKU pairs
        sqlContext.sql(
            """
             SELECT concat(a.sku1, '_', a.sku2) as pairID,
                    a.sku1,
                    a.sku2,
                    a.pv1,
                    a.pv2,
                    a.comPV,
                    a.totalPV,
                    b.SubSubCategory as ssc1,
                    c.SubSubCategory as ssc2,
                    CASE WHEN cast(b.FirstVisibleDate as date) > cast(c.FirstVisibleDate as date)
                         THEN cast(b.FirstVisibleDate as date)
                         ELSE cast(c.FirstVisibleDate as date) end  as maxvd
             FROM skuPairViewsFiltDF a
                 JOIN skuVisibleDF b
                     ON a.sku1 = b.ProductItemID
                 JOIN skuVisibleDF c
                     ON a.sku2 = c.ProductItemID
            """).cache().registerTempTable('skuPairInfDF')


        # Finding the aggregate pageviews for the two subsubcats of the sku Pair
        # for the days after the time both are visible
        sqlContext.sql(
            """
              SELECT F.pairID
                   ,sum(VisitCount_ssc1) as AggVisitCount_ssc1
                   ,sum(VisitCount_ssc2) as AggVisitCount_ssc2
              FROM (
                       SELECT D.pairID,
                              cast(D.VisitCount as int) as VisitCount_ssc1,
                              cast(E.VisitCount as int) as VisitCount_ssc2
                        FROM
                               (SELECT a.pairID,
                                       b.VisitCount,
                                       b.VisitDate
                                FROM skuPairInfDF a
                                  INNER JOIN dateSubCatDF b
                                      on a.ssc1 = b.SubSubCategory
                                      and b.VisitDate >= a.maxvd
                                ) D
                        INNER JOIN
                                ( SELECT a.pairID,
                                         b.VisitCount,
                                         b.VisitDate
                                  FROM skuPairInfDF a
                                      INNER JOIN dateSubCatDF b
                                          on a.ssc2 = b.SubSubCategory
                                          and b.VisitDate >= a.maxvd
                                ) E
                                on D.pairID = E.pairID
                                    and D.VisitDate = E.VisitDate
                   ) F
             GROUP BY F.pairID
            """).cache().registerTempTable('skuPairPVcount')

        # Merging all the relevant data to be passed for calcluating PMI
        sqlContext.sql(
            """
            SELECT  a.sku1,
                    a.sku2,
                    a.pv1,
                    a.pv2,
                    a.comPV,
                    a.totalPV,
                    c.AggVisitCount as potPV1,
                    d.AggVisitCount as potPV2,
                    CASE WHEN b.AggVisitCount_ssc1 > b.AggVisitCount_ssc2 THEN b.AggVisitCount_ssc2
                        ELSE b.AggVisitCount_ssc1
                        END as minAggVisitCount
            FROM skuPairInfDF a
                INNER JOIN skuPairPVcount b
                    ON a.pairID = b.pairID
                INNER JOIN aggPVcount c
                    ON a.sku1 = c.ProductItemID
                INNER JOIN aggPVcount d
                    ON a.sku2 = d.ProductItemID
            """).coalesce(1).write.format('com.databricks.spark.csv').\
                           options(header='true').\
                           save('skuPair.csv')

    def calcPMI(self):
        """
        :param mtype: determine the normalization method for PMI
        :return: write space delimited file (edgesW.txt) of network edges to hard drive
        format of output data: sourceSKU targetSKU weight (which is the PMI)
        """
        start_time = time.time()

        import csv
        import sys #used for passing in the argument
        file_name = sys.argv[1] #filename is argument 1
        # ****** FIX THE FILE ADDRESS
        with open('./skuPair.csv/part-00000', 'rU') as f:  #opens PW file ~/projects/pvclus/skuPair.csv/part-00000.csv'
            reader = csv.reader(f)
            skuPair = list(list(rec) for rec in csv.reader(f, delimiter=',')) #reads csv into a list of lists

        # skuPair contains the visits and potential visits for each SKU pair
        skuPair = skuPair[1:]   # deleting the column ID


        # calculating PMI measures for each pair of SKU
        if self.norm == 1:   # normalized PMI adjusted to (-100,100) range
            # edgesW = [[r[0], r[1], (1 + math.log((r[4]*r[5]/r[3]*r[2] + 1e-20), 2) / -math.log((r[4]/r[5] + 1e-20), 2))*100/2]
            edgesW = [[r[0], r[1], (math.log(( (float(r[4])*r[6]*r[7])/(r[3]*r[2]*r[8]) + 1e-40), 2) / -math.log((float(r[4])/r[8] + 1e-40), 2))*100]
                 for r in skuPair]
        elif self.norm == 2:   # powered PMI - UPDATE THIS
            edgesW = [[r[0], r[1], 2**(math.log((float(r[4])*r[5]/(r[3]*r[2]) + 1e-40), 2) + math.log((r[4]/r[5] + 1e-20), 2))]
                 for r in skuPair]
        elif self.norm == 3:   # PMI
            # edgesW = [[r[0], r[1], math.log((r[4]*r[5]/r[3]*r[2] + 1e-40), 2)] for r in skuPair]
            edgesW = [[r[0], r[1], math.log(( (float(r[4])*r[6]*r[7])/(r[3]*r[2]*r[8]) + 1e-40), 2)] for r in skuPair]
        elif self.norm == 4:   # PMI^2
            # edgesW = [[r[0], r[1], math.log((r[4]*r[4]/r[3]*r[2] + 1e-40), 2)] for r in skuPair]
            edgesW = [[r[0], r[1], math.log((float(r[4])**2*r[6]*r[7]/(r[3]*r[2]*r[8]**2) + 1e-40), 2)] for r in skuPair]
        elif self.norm == 5:   # PMI^3
            edgesW = [[r[0], r[1], math.log((float(r[4])**3*r[6]*r[7]/(r[3]*r[2]*r[8]**3) + 1e-40), 2)] for r in skuPair]
        else:
            print "Improper PMI normalization index"

        # Filtering based on the pmi Threshold
        df = pandas.DataFrame(edgesW)
        df.columns = ['source', 'target', 'weight']
        pmiPercThresh = numpy.percentile(df[df['weight']>0]['weight'],self.pmiPerc)
        print 'Cut off percentile value', pmiPercThresh
        print 'Before PMI threshold filter: ', df.describe()


        # Dropping ones with zero or negative weight:
        if self.method != 7:
            edgesW = [[a[0], a[1], a[2]] for a in edgesW if a[2] > pmiPercThresh]
        else:  # shifting PMI down by self.pmiThresh for the spinglass comm detection - requiring stronger links for community
            edgesW = [[a[0], a[1], (a[2] - pmiPercThresh)] for a in edgesW]

        print "Number of remaining edges after filtering for pmiThresh: ", len(edgesW)

        # printing the summary statistics of the PMI values
        df = pandas.DataFrame(edgesW)
        #df = df.transpose()
        df.columns = ['source', 'target', 'weight']
        print 'After PMI threshold filter: ', df.describe()

        # write to space delimited file to import to as a graph
        openfile = open('edgesW.txt', 'w')
        for row in edgesW:
             openfile.write("%s\n" % " ".join(str(x) for x in row))
        openfile.close()
        print("--- %s seconds --- Formatting data into pairs & calculating probabilities" % (time.time() - start_time))

    def commDetect(self, method, subComm = None, sg_gamma = None):
        """
        Read the edges file, convert it to graph items
        Run community detection algorithm based on selected method
        Option of creating the graph of clusters
        :param
            method: determine the community detection alg., k: cluster size (optional)
            subComm: graph data to be used to create clusters - when None the edgesW.txt file on hard is used
            sg_gamma: gamma factor for the spin glass comm detection algorithm - when None the default gamma is used
        :return:
            clusTable: cluster membership table, format: dictionary {sku: clusterID}
            counter: {cluster IDs: cluster sizes}
            community: the item that captures the structure of the community of clusters after
                applying the comm detection algorithm
        """

        # Reading the files for community detection
        if subComm is None:
            g = igraph.Graph.Read_Ncol('edgesW.txt', weights=True, directed=False)
            method = self.method
        else:
            g = subComm
            print 'method specified by the sub-clustering procedure:', method

        if sg_gamma is None:
            sg_gamma = self.sg_gamma

        g.simplify(multiple=True, loops=True, combine_edges=max)   # Remove loops and multiple edges keep the maximum weight
        # print "Number of SKUs", len(g.vs['name'])
        # save the graph data in Python pickle format
        g.write_pickle(fname='graphDat')

        if method == 1:
            communities = Graph.community_fastgreedy(g, weights=g.es['weight'])
        elif method == 2:
            communities = Graph.community_walktrap(g, weights=g.es['weight'], steps=8)
        elif method == 3:
            communities = Graph.community_leading_eigenvector(g, weights=g.es['weight'], clusters=None, arpack_options=None)
        elif method == 4:
            communities = Graph.community_multilevel(g, weights=g.es['weight'], return_levels=False)
        elif method == 5:
            communities = Graph.community_edge_betweenness(g, weights=g.es['weight'], clusters=None, directed=False)
        elif method == 6:
            communities = Graph.community_label_propagation(g, weights=g.es['weight'], initial=None, fixed=None)
        elif method == 7:  # Only one allowing for negative weights
            communities = Graph.community_spinglass(g,
                                                    weights=g.es['weight'],
                                                    implementation="neg",
                                                    spins=75,
                                                    gamma=sg_gamma,
                                                    lambda_=self.sg_lambda)
        elif method == 8:  # Spin glass only on positive links
            communities = Graph.community_spinglass(g,
                                                    weights=g.es['weight'],
                                                    implementation="orig",
                                                    spins=75,
                                                    gamma=sg_gamma)
        else:
            print "Improper community detection method code is provided"

        if method in (1,2,5):  # for community detections returning VertexDendrogram
            clusters = communities.as_clustering(self.k)    # we can set the number of clusters here self.k
            members = clusters.membership
            members = [m+1 for m in members]
            numClus = len(clusters)
            print members, numClus


        if method in (3,4,6,7,8):  # for community detections returning VertexClustering
            members = communities.membership
            members = [m+1 for m in members]
            numClus = communities._len
            print 'Number of Clusters:', numClus, ' --- Modularity:', communities.modularity # members,

        # finding the size of clusters
        counter = collections.Counter(members)
        print 'Cluster sizes: ', counter

        clusTable = dict(zip(g.vs['name'], members))
        # print clusTable

        # To plot the graph when running the comm clustering
        # self.plotGraph(g, members, numClus)

        return clusTable, counter, communities

    def subComm(self):
        """
        Function breaks the large communities into smaller communities using SpinGlass community detection method.
        The gamma parameter is increased till we arrive at the cluster sizes below the threshold or the specified
        maximum number of interation is reached.
        It calls the self.commDetect again for the clusters > self.clSizeThresh and break the cluster.
        :return:
        clusTable: dictionary of {sku:clusterID}
        counter: dictionary of {clusterId:cluster size}
        """
        start_time = time.time()
        clusTable, counter, communities = self.commDetect(method=self.method)

        # TO ASSURE NO cLUSTER HAS large size we can loop it here more than once
        # If the largest cluster is larger than the allowed threshold
        mergedClusTable = defaultdict(list)

        for idx, clSize in counter.iteritems():
            sg_gamma = self.sg_gamma
            beshmor = 1
            if clSize > self.clSizeThresh:
                # Run this till cluster size is good or max iterations is reached
                print '\n', 'Cluster ID to be broken: ', idx
                while True:
                    print 'gamma: ', sg_gamma, 'iteration: ', beshmor
                    subComm = communities.subgraph(idx=(idx-1))  # detaching the large community
                    subClusTable, subCounter, subCommunities = self.commDetect(subComm=subComm,
                                                                               method=self.subMethod,
                                                                               sg_gamma=sg_gamma)
                    sg_gamma += 0.5
                    beshmor += 1
                    if ((max(subCounter.values()) <= self.clSizeThresh) or beshmor>self.iterCount):
                        break
                mergedClusTable.update(subClusTable)
            else:
                subComm = communities.subgraph(idx=(idx-1))  # -1 to compensate fore the 1 unit increase in index on self.commDetect
                mergedClusTable.update(dict(zip(subComm.vs['name'], [0]*len(subComm.vs['name']))))

        # Merging the clusters sub-ID wih the previous step cluster ID
        mergedClusTable2 = defaultdict(list)
        for d in (clusTable, mergedClusTable):
            for key, value in d.iteritems():
                mergedClusTable2[key].append(value)

        # Making an integer number out of the list of cluster IDs to be used as new cluster IDS
        for sku, ind in mergedClusTable2.iteritems():
            mergedClusTable2[sku] = "-".join(map(str, flatten(ind)))  # flatten(ind): function to flatten nested list of lists

        # assigning the new sub cluster sizes.
        counter = collections.Counter(mergedClusTable2.values())

        print("--- %s seconds --- Clustering Process " % (time.time() - start_time))

        # We need to use the updated clustering data only if there has been a large cluster
        print 'mergedClusTable2 & counter -->', mergedClusTable2, counter
        return mergedClusTable2, counter

    def skuClusImgTab(self):
        """
        make a table of SKU, ClusterID, Subsubcat, SKU Image URL
        to be used as input to Markdown to create HTML table for quality control
        :return: write CSV file of above format on hard-drive to be read and displayed by R markdown
        """

        clusTable, counter = self.subComm()

        # Running the stored proc. to collect image URL and subsubcat data for all sku
        # Efficiency note: probably can read from a table where sku number is matching when sku set gets large
        conn = pymssql.connect(server=self.ODBC_PROD,
                                database='DWStage',
                                user=cred.user,
                                password=cred.pw)
        readerFunc = conn.cursor(as_dict=False)
        readerFunc.callproc('SP_FetchSKUImgURL')
        skuImgPath = {}
        for r in readerFunc:
            skuImgPath.update({str(r[0]): [r[2], r[1]]})

        # Create a dictionary with key: SKU, item:[cluster code, subsubcat, ImgURL]
        clusImgDic = {}
        for key,value in clusTable.iteritems():
            if key in skuImgPath:
                clusImgDic[key] = [value,skuImgPath[key][0],skuImgPath[key][1]]
            else:
                clusImgDic[key] = [value,None,None]

        clusImgTab = []
        for k, v in clusImgDic.iteritems():
            clusImgTab.append([k,v[0],v[1],v[2]])

        # write the CSV file to be used by R mark down.
        with open("clusPV_"+
                    str(self.sscat)+'_'+
                    str(self.method)+'_'+
                    str(self.subMethod)+'_'+
                    str(self.pmiPerc)+".csv", "wb") as f:
            writer = csv.writer(f)
            writer.writerows(clusImgTab)

        conn.close()
        return counter

    def plotGraph(self, g, members, numOfClus):
        """
        generating the graph of vertices and edges
        :param g: graph file to be plotted
        :param members: vector of membership (size = # of SKUs)
        :param numOfClus: number of clusters
        :return: None
        """

        # Setting up the ends of the spectrum
        lime = Color("lime")
        red = Color("red")
        cols = list(red.range_to(lime, numOfClus))
        cols = [str(rang).split()[0] for rang in cols]

        colDict = dict(zip(range(numOfClus), cols))
        print colDict
        # [colDict[m] for m in members]

        visual_style = {}
        visual_style["vertex_size"] = 80
        visual_style["vertex_shape"] = "rectangle"
        visual_style["vertex_color"] = [colDict[m] for m in members]   #[color_dict[gender] for gender in g.vs["gender"]]
        visual_style["vertex_label"] = g.vs['name']
        visual_style["edge_width"] = [w/10 for w in g.es['weight']] #[1 + 2 * int(is_formal) for is_formal in g.es["is_formal"]]
        # visual_style["layout"] = layout
        visual_style["bbox"] = (5500, 4000)
        # visual_style["margin"] = 120
        visual_style["edge_curved"] = False

        plot(g, **visual_style).save(fname= 'graphClus_' +
                                            str(self.timeThresh) + '_' +
                                            str(self.jointViewThresh) + '_' +
                                            str(self.pmiPerc) + '_' +
                                            str(self.method) + '_' +
                                            str(self.sg_lambda))  # , target="diagram.svg" layout = layout,
        # dendPlot(g)


class ProductionPVCluster(object):
    """
    class for functions required to productionize the PV color cluster process
    we may put some back into the original class
    """
    def __init__(self,
                 startDate,
                 endDate,
                 aggPVThresh):
        self.startDate = startDate
        self.endDate = endDate
        self.aggPVThresh = aggPVThresh

        self.ODBC_PROD = 'DBSQLSVR01'
        self.ODBC_DEV = 'V1DEDB01'
        self.db = 'DW0'

    def readProdCat(self):
        """
        This file reads the ((sub)sub)category of products to do the clustering upon
        :return:
        a list of subCategories
        """
        conn = pymssql.connect(server = self.ODBC_PROD,
                                database = self.db,
                                user = cred.user,
                                password = cred.pw)
        readerFunc = conn.cursor(as_dict = False)
        readerFunc.execute("""
                            ;WITH cte1 as  (
                            SELECT DISTINCT [SubCategory],
                                COUNT(*) as freq
                            FROM [DM0].[dbo].[Products]
                            WHERE visibleOnWeb=1
                            GROUP BY SubCategory
                            )
                            SELECT SubCategory FROM cte1 WHERE freq > %d
                            """ %(self.aggPVThresh))
        subCats = readerFunc.fetchall()
        return subCats


if __name__ == '__main__':
    print "Start date and time: " + time.strftime("%c")
    start_time = time.time()
    run = ProductionPVCluster(startDate='2016-01-01',
                               endDate='2016-05-01',
                               aggPVThresh=200)  # Parameter to set minumum unumber of PV for a subCat to be included in analysis

    subCats = run.readProdCat()
    print subCats
    # recording the cluster sizes for each run of the model
    tests = {}
    # Two subcats not working - test them , this is a temp solution, subcats are derived from the DB above
    for sc in subCats:
        print '\n','\n','\n','*********************************'
        print '***** ANALYSIS ON: ', sc[0]
        vals = [50] # [10,25,50,75,85]
        for case in vals:
            pvclus = PVCluster(startDate='2016-01-01',
                               endDate='2016-05-01',
                               catLevel='b.SubCategory',
                               sscat=sc[0],
                               timeThresh=10,
                               minViewThresh=3,
                               jointViewThresh=1,
                               pmiPerc=case,
                               norm=1,
                               method=4,
                               k=None,
                               sg_gamma=0.5,
                               sg_lambda=0.5,
                               clSizeThresh=30,
                               subMethod=8,
                               iterCount=5)

            pvclus.calcPMI()   # read data calculate PMI
            tests['Case_'+str(sc)+str(case)] = pvclus.skuClusImgTab()  # Create clusters and put them in tables

    print tests
    print("--- %s seconds --- Complete Clustering Process on All SubCats: " % (time.time() - start_time))
