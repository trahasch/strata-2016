
# coding: utf-8

# ## Recommendation

# ###1. Read Movielens 1 Million Data (Medium)
# ###2. Partition data into Train, Validation & Test Datasets (60-20-20)
# ###3. Train ALS Recommender
# ###4. Measure Model Performance
# ###5. Optimize Model parameters viz. Rank, Lambda & Number Of Iterations based on the Validation Dataset
# ###6. Predict optmized Model performance (RMSE) on the Test Data

# In[10]:

from pyspark.context import SparkContext
print "Running Spark Version %s" % (sc.version)


# In[11]:

from pyspark.conf import SparkConf
conf = SparkConf()
print conf.toDebugString()


# In[12]:

movies_file = sc.textFile("movielens/medium/movies.dat")
movies_rdd = movies_file.map(lambda line: line.split('::'))
movies_rdd.count()


# In[13]:

movies_rdd.first()


# In[14]:

ratings_file = sc.textFile("movielens/medium/ratings.dat")
ratings_rdd = ratings_file.map(lambda line: line.split('::'))
ratings_rdd.count()


# In[15]:

ratings_rdd.first()


# In[16]:

def parse_ratings(x):
    user_id = int(x[0])
    movie_id = int(x[1])
    rating = float(x[2])
    timestamp = int(x[3])/10
    return [user_id,movie_id,rating,timestamp]


# In[17]:

ratings_rdd_01 = ratings_rdd.map(lambda x: parse_ratings(x))
ratings_rdd_01.count()


# In[18]:

ratings_rdd_01.first()


# In[19]:

numRatings = ratings_rdd_01.count()
numUsers = ratings_rdd_01.map(lambda r: r[0]).distinct().count()
numMovies = ratings_rdd_01.map(lambda r: r[1]).distinct().count()

print "Got %d ratings from %d users on %d movies." % (numRatings, numUsers, numMovies)


# ###A quick scheme to partition the training, validation & test datasets
# #### Timestamp ending with [6,8) = Validation
# #### Timestamp ending with [8,9] = Test (ie >= 8)
# #### Rest = Train
# 
# #### Approx: Training = 60%, Validation = 20%, Test = 20%

# #Coding Exercise
# ### Partition Data

# In[20]:

import time
start_time = time.time()
training = ratings_rdd_01.filter(lambda x: (x[3] % 10) < 6)
validation = ratings_rdd_01.filter(lambda x: (x[3] % 10) >= 6 and (x[3] % 10) < 8)
test = ratings_rdd_01.filter(lambda x: (x[3] % 10) >= 8)
numTraining = training.count()
numValidation = validation.count()
numTest = test.count()
print "Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest)
print "Elapsed : %f" % (time.time() - start_time)


# In[21]:

from pyspark.mllib.recommendation import ALS
rank = 10
numIterations = 20
train_data = training.map(lambda p: (p[0], p[1], p[2]))
start_time = time.time()
model = ALS.train(train_data, rank, numIterations)
print "Elapsed : %f" % (time.time() - start_time)
print model


# ###In order to calculate model performance we need a keypair with key=(userID, movieID), value=(pred,actual)
# #### Then we can do calculations on the Predicted vs Actual values

# In[22]:

# Evaluate the model on validation data
validation_data = validation.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(validation_data).map(lambda r: ((r[0], r[1]), r[2]))
predictions.count()


# In[23]:

predictions.first()


# ###Now let us turn the Validation data to KV pair

# In[24]:

validation_data.first()


# In[25]:

validation.first()


# In[26]:

validation_key_rdd = validation.map(lambda r: ((r[0], r[1]), r[2]))
print validation_key_rdd.count()
validation_key_rdd.first()


# In[27]:

#ratesAndPreds = validation.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
ratesAndPreds = validation_key_rdd.join(predictions)
ratesAndPreds.count()


# In[28]:

ratesAndPreds.first()


# ### Now we have the values where we want them !

# In[29]:

MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).reduce(lambda x, y: x + y)/ratesAndPreds.count()
print("Mean Squared Error = " + str(MSE))


# In[30]:

# 1.4.0 Mean Squared Error = 0.876346112824
# 1.3.0 Mean Squared Error = 0.871456869392
# 1.2.1 Mean Squared Error = 0.877305629074


# # Advanced - to try later *** system will hang if it has less memory

# ### Validation Run
# #### Let us use the Validation Data to optimize Rank, Lambda & Number Of Iterations
# #### And Predict the model performance using our test data

# In[31]:

def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2]))       .join(data.map(lambda x: ((x[0], x[1]), x[2])))       .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))


# In[32]:

import itertools
from math import sqrt
from operator import add
ranks = [8, 12]
lambdas = [0.1, 1.0, 10.0]
numIters = [10, 20]
bestModel = None
bestValidationRmse = float("inf")
bestRank = 0
bestLambda = -1.0
bestNumIter = -1
start_time = time.time()
for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
    model = ALS.train(train_data, rank, numIter, lmbda)
    validationRmse = computeRmse(model, validation, numValidation)
    print "RMSE (validation) = %f for the model trained with " % validationRmse +           "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, numIter)
    if (validationRmse < bestValidationRmse):
        bestModel = model
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lmbda
        bestNumIter = numIter

testRmse = computeRmse(bestModel, test, numTest)

# evaluate the best model on the test set
print "Best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda)   + "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse)
print "Elapsed : %f" % (time.time() - start_time)


# In[ ]:



