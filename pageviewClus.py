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
Created: Sina G, March 2016
Updates:
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
        :return: 1. set Dictionary {skuID_j:{sessionID_1, ..., sessionID_i (visitorID in DB)}
                 for all viewed skus - skuID is the key of dict and item is a set of sesssions
                 2. grand total number of distinct sessions
        """
        start_time = time.time()
        # read SKU number and the VisitorID
        conn = pymssql.connect(server=self.ODBC_PROD,
                                database=self.db,
                                user=cred.user,
                                password=cred.pw)
        readerFunc = conn.cursor(as_dict=False)
        readerFunc.execute("""
                            SELECT a.[ProductItemID]
                                  ,a.[VisitorId]
                            FROM DW0.dbo.AdobePageViewBySession a
                            		INNER JOIN DM0.dbo.Products b
			                            on a.ProductItemID = b.ProductItemID
                            WHERE a.TimeSpentOnPage > %d
                                and a.VisitDate > '%s'
                                and a.VisitDate < '%s'
                                and b.VisibleOnWeb = 1
                                and %s = '%s'
                            ORDER BY a.[ProductItemID]
                            """  %(self.timeThresh,
                                   self.startDate,
                                   self.endDate,
                                   self.catLevel,
                                   self.sscat))

        # Generating dictionary with SKU as key and all VisitorIds (sessions) as a set
        skuVisits = {}
        for r in readerFunc:
            if r[0] in skuVisits.keys():
                skuVisits[r[0]].update(set([r[1]]))
            else:
                skuVisits[r[0]] = set([r[1]])

        skuVisitsFiltered = {}
        for sku, visits in skuVisits.iteritems():
            if len(visits) > self.minViewThresh:
                skuVisitsFiltered[sku] = visits

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
        return skuVisitsFiltered, totalVisits

    def createDenomData(self):
            """
            Called by self.calcPMI
            Read total visit (visitor_ID per day) number for each SKU and each SKU pair
            :return: skuPair (sku1, sku2, visits_sku1, visits_sku2, grand total visits)
                     appended with the potential pageview for SKUs and their joint view
                     based on the subsubcategory (minimum of two sscat for pairs) and
                     the dates SKUs (both for pairs) were visible on web
            """

            start_time = time.time()
            # read SKU number and the VisitorID
            conn = pymssql.connect(server = self.ODBC_PROD,
                                    database = self.db,
                                    user = cred.user,
                                    password = cred.pw)
            readerFunc = conn.cursor(as_dict = False)
            readerFunc.execute("""
                                SELECT a.[ProductItemID],
                                     max(b.FirstVisibleDate) FirstVisibleDate,
                                    --	b.Category,
                                    --  min(b.SubCategory)
                                    	min(b.SubSubCategory)
                                FROM DW0..[AdobePageViewBySession] a
                                        INNER JOIN DM0..Products b
                                            on a.ProductItemID = b.ProductItemID
                                WHERE a.TimeSpentOnPage > %d
                                        and a.VisitDate > '%s'
                                        and a.VisitDate < '%s'
                                        and b.VisibleOnWeb = 1
                                        and %s = '%s'
                                GROUP BY a.ProductItemID
                                ORDER BY a.ProductItemID
                               """ %(self.timeThresh,
                                     self.startDate,
                                     self.endDate,
                                     self.catLevel,
                                     self.sscat))
            skuVisible = readerFunc.fetchall()

            readerFunc.execute("""
                                SELECT count(distinct  a.[UserId]) as VisitCount,
                                          convert (date, a.VisitDate) as VisitDate,
                                        --	b.Category,
                                        --  b.SubCategory
                                        	b.SubSubCategory
                                FROM DW0..[AdobePageViewBySession] a
                                    INNER JOIN DM0..Products b
                                        on a.ProductItemID = b.ProductItemID
                                WHERE a.TimeSpentOnPage > %d
                                    and a.VisitDate > '%s'
                                    and a.VisitDate < '%s'
                                    and b.VisibleOnWeb = 1
                                    and %s = '%s'
                                GROUP BY b.SubSubCategory, convert (date, a.VisitDate)
                                ORDER BY b.SubSubCategory, VisitDate
                                """ %(self.timeThresh,
                                      self.startDate,
                                      self.endDate,
                                      self.catLevel,
                                      self.sscat))
            # Generating dictionary with SKU as key and all VisitorIds (sessions) as a set
            dateSubcat = readerFunc.fetchall()

            # create dictionary with SKU as the key to call the corresponding values
            #
            skuPotVisits = {}
            skuActive = {}
            for sku in skuVisible:  # could only search in the required SKU passed through the file to make faster
                skuPotVisits[sku[0]] = sum([x[0] for x in dateSubcat if x[2] == sku[2] and
                                            time.strptime(x[1], "%Y-%m-%d") > time.strptime(sku[1], "%Y-%m-%d")])
                skuActive[sku[0]] = [sku[1], sku[2]]

            with open('skuPairInf.txt', 'r') as file:
                skuPair = json.load(file)

            for skus in skuPair:
                skus.append(skuPotVisits.get(skus[0]))
                skus.append(skuPotVisits.get(skus[1]))

                # Spit the list into two dictionaries to find the minimum visits for the two subcat in the next step
                dateDict1 = {}  # Efficiency note: we can make this faster by bypassing it when subcats agree
                dateDict2 = {}
                for date in dateSubcat:
                    if skuActive[skus[0]][1] == date[2]:  # check the subcat of date data matching one of the SKU's
                        dateDict1[date[1]] = date[0]
                    if skuActive[skus[1]][1] == date[2]:
                        dateDict2[date[1]] = date[0]

                visitCounts = [0]
                for date in dateSubcat:  # Efficiency note: we could order the date and collect all after the desired date instead of looping over all dates
                    if (time.strptime(date[1], "%Y-%m-%d") > time.strptime(skuActive[skus[0]][0], "%Y-%m-%d")):
                         if (time.strptime(date[1], "%Y-%m-%d") > time.strptime(skuActive[skus[1]][0], "%Y-%m-%d")):
                             # int(value or 0) will use 0 in the case when you provide any value that Python considers False, such as None, 0, [], "", etc.
                             # alternatively we can use the following for readability: int(0 if value is None else value)
                             # or alternatively: min(filter(None, [dateDict1.get(date[1]), dateDict2.get(date[1])]))
                             visitCounts.append(int(min(dateDict1.get(date[1]), dateDict2.get(date[1])) or 0))
                skus.append(sum(visitCounts))

            # print skuPair

            print("--- %s seconds --- Finding the potential visits for each SKU " % (time.time() - start_time))
            conn.close()
            return skuPair

    def calcPMI(self):
        """
        Reads skuVisits, totalVisits from readData() function
        Calculate the pointwise mutual information (PMI) for each pair of SKU
        with joint visit > jointVisitThresh for both (s_1,s_2) and (s_2,s_1)
        :param mtype: determine the normalization method for PMI
        :return: write space delimited file (edgesW.txt) of network edges to hard drive
        format of output data: sourceSKU targetSKU weight (which is the PMI)
        """
        start_time = time.time()
        # Taking the inpot
        skuVisits, totalVisits = self.readData()
        # creating pairs of SKU, their session count, their shared session count
        skuPair = []
        for skus in itertools.combinations(skuVisits.keys(),2):
            skuPair.append([skus[0],
                            skus[1],
                            len(skuVisits[skus[0]]),
                            len(skuVisits[skus[1]]),
                            len(skuVisits[skus[0]].intersection(skuVisits[skus[1]])),
                            totalVisits])

        skuPair = [r for r in skuPair if r[4] > self.jointViewThresh]  # We can pick this threshold for minimum share click-streams based on reasoning and computation time
        print "Number of SKU pairs used for analysis: ", len(skuPair)
        with open('skuPairInf.txt', 'w') as file:
            json.dump(skuPair, file)

        # Generate and Read the potential visits for each SKU and SKU pair
        skuPair = self.createDenomData()

        # openfile = open('PreedgesW.txt', 'w')  # For testing purposes feel free to drop - also you can check if all pairs remain in the final analysis as they dropped for no obvious reason in one try
        # for row in skuPair:
        #      openfile.write("%s\n" % " ".join(str(x) for x in row))
        # openfile.close()

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
        :param method: determine the community detection alg., k: cluster size (optional)
        :return: clusTable: cluster membership table, format: dictionary {sku: clusterID}
        """
        start_time = time.time()
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
        print("--- %s seconds --- Clustering Process " % (time.time() - start_time))

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
    #comment
    run = ProductionPVCluster(startDate='2016-01-01',
                               endDate='2016-05-01',
                               aggPVThresh=100)  # Parameter to set minumum unumber of PV for a subCat to be included in analysis

    subcats = run.readProdCat()
    tests = {}
    # Two subcats not working - test them , this is a temp solution, subcats are derived from the DB above
    for sc in  [# 'Accessories',
                # 'Accessory',
                # 'Carpet & Rugs',
                # 'Decking',
                # 'Doors',
                # 'Wood Flooring',
                # 'Tile Flooring',
                # 'Vinyl Flooring',
                # 'Wall Tile & Mosaics',
               ## 'Furniture', ---> catch error - in createDenom
                # 'Kitchen & Bath',
                # 'Landscape',
                'Lighting',
                'Millwork',
                'Moldings & Trims',
                'Outdoor',
                'Outdoor Accessories',
                'Siding',
                'Sinks']:
        print '\n','\n','\n','*********************************'
        print '***** ANALYSIS ON: ', sc
        vals = [50] # [10,25,50,75,85]
        for case in vals:
            pvclus = PVCluster(startDate='2016-01-01',
                               endDate='2016-05-01',
                               catLevel='b.SubCategory',
                               sscat=sc,
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
            tests['Case_'+str(sc)+str(case)] = pvclus.skuClusImgTab()

            # tests['Case_'+str(sc)] = out2  # collect cluster sizes for all runs

        #    pvclus.commDetect()    # do comm detection - redundant if already called skuClusImgTab()
        #    pvclus.skuClusImgTab()
        #    pvclus.subComm()

         #   tests['Case_'+str(case)] = pvclus.skuClusImgTab() # do comm detection and write image tab
    #    pvclus.createDenomData()  # for test; this is called by calcPMI

    print tests
    print("--- %s seconds --- Complete Clustering Process on All SubCats: " % (time.time() - start_time))
