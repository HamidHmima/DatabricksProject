# Databricks notebook source
#import packages
 
from pyspark.sql.functions import explode
import pyspark.sql.functions as f
from pyspark.sql.functions import explode, array
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, array
from pyspark.sql.functions import isnan, when, count, col
from delta.tables import*

# COMMAND ----------

spark.conf.set("fs.azure.account.key.moviesrecomhmimasa.dfs.core.windows.net","TCmHPF0RelStqP6nJ5LTUHe71aTfPy9buy7+znY9Vl/j9Gcc8Q0w1wmFI1On14Rb/ck2YO+YfIY8+AStwPAFjw==")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

#Pour améliorer les performances lors des lectures répétées, nous avons activé la cache du disque
spark.conf.set("spark.databricks.io.cache.enabled", "true")

dbutils.fs.ls("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/")

# COMMAND ----------

dbutils.fs.ls("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/")

# COMMAND ----------

df_business = spark.read.json("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_business.json")
df_business.write.mode('overwrite').parquet("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_business.paquet")
df_review = spark.read.json("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_review.json")
df_review.write.mode('overwrite').parquet("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_review.paquet")
#df_tip = spark.read.json("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_tip.json")
#df_tip.write.mode('overwrite').parquet("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_tip.paquet")
df_user = spark.read.json("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_user.json")
df_user.write.mode('overwrite').parquet("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_user.paquet")

# COMMAND ----------

#read parquet file 
df_business = spark.read.parquet("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_business.paquet")
df_review = spark.read.parquet("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_review.paquet")
#df_tip = spark.read.parquet("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_tip.paquet")
df_user = spark.read.parquet("abfss://moviesrecomhmimacontainer@moviesrecomhmimasa.dfs.core.windows.net/yelp_academic_dataset_user.paquet")

# COMMAND ----------

print("df_business: " + str(df_business.count()))
print("df_review: " + str(df_review.count())) 
print("df_user: " + str(df_user.count()))

# COMMAND ----------

df_business.toPandas().to_csv("/dbfs/FileStore/backup.csv")

# COMMAND ----------

df_business.printSchema()

# COMMAND ----------

df_review.printSchema()

# COMMAND ----------

df_user.printSchema()

# COMMAND ----------

df_business_cat= df_business.groupBy("categories").agg(f.count("review_count").alias("total_review_count"))
window = Window.partitionBy(df_business_cat['categories']).orderBy(df_business_cat['total_review_count'].desc())
df_top_categories = df_business_cat.select('*', rank().over(window).alias('rank'))
df_business_cat.createOrReplaceTempView("business_categories")

# COMMAND ----------

df_business.createOrReplaceTempView("business")
df_review.createOrReplaceTempView("reviews")
df_user.createOrReplaceTempView("users")

# COMMAND ----------


# Nuos avnons remplacé les valeurs nulles dans les colonnes StructType par des valeurs vides pour Hours et attributes.

df_business = df_business.withColumn(
    "hours",
    f.coalesce(
        f.col("hours"),
        f.struct(
            f.lit("").alias("Friday"),
            f.lit("").alias("Monday"),
            f.lit("").alias("Saturday"),
            f.lit("").alias("Sunday"),
            f.lit("").alias("Thursday"),
            f.lit("").alias("Tuesday"),
            f.lit("").alias("Wednesday"),
        ),
    ),
)

# COMMAND ----------

df_business = df_business.withColumn(
    "attributes",
    f.coalesce(
        f.col("attributes"),
        f.struct(
            f.lit(None).alias("AcceptsInsurance"),
            f.lit(None).alias("AgesAllowed"),
            f.lit(None).alias("Alcohol"),
            f.lit(None).alias("Ambience"),
            f.lit(None).alias("BYOB"),
            f.lit(None).alias("BYOBCorkage"),
            f.lit(None).alias("BestNights"),
            f.lit(None).alias("BikeParking"),
            f.lit(None).alias("BusinessAcceptsBitcoin"),
            f.lit(None).alias("BusinessAcceptsCreditCards"),
            f.lit(None).alias("BusinessParking"),
            f.lit(None).alias("ByAppointmentOnly"),
            f.lit(None).alias("Caters"),
            f.lit(None).alias("CoatCheck"),
            f.lit(None).alias("Corkage"),
            f.lit(None).alias("DietaryRestrictions"),
            f.lit(None).alias("DogsAllowed"),
            f.lit(None).alias("DriveThru"),
            f.lit(None).alias("GoodForDancing"),
            f.lit(None).alias("GoodForKids"),
            f.lit(None).alias("GoodForMeal"),
            f.lit(None).alias("HairSpecializesIn"),
            f.lit(None).alias("HappyHour"),
            f.lit(None).alias("HasTV"),
            f.lit(None).alias("Music"),
            f.lit(None).alias("NoiseLevel"),
            f.lit(None).alias("Open24Hours"),
            f.lit(None).alias("OutdoorSeating"),
            f.lit(None).alias("RestaurantsAttire"),
            f.lit(None).alias("RestaurantsCounterService"),
            f.lit(None).alias("RestaurantsDelivery"),
            f.lit(None).alias("RestaurantsGoodForGroups"),
            f.lit(None).alias("RestaurantsPriceRange2"),
            f.lit(None).alias("RestaurantsReservations"),
            f.lit(None).alias("RestaurantsTableService"),
            f.lit(None).alias("RestaurantsTakeOut"),
            f.lit(None).alias("Smoking"),
            f.lit(None).alias("WheelchairAccessible"),
            f.lit(None).alias("WiFi"),
        ),
    ),
)

# COMMAND ----------

 df_business = df_business.na.fill("None",("categories"))

# COMMAND ----------

df_business.select([count(when(col(c).isNull(), c)).alias(c) for c in df_business.columns]).show()

# COMMAND ----------

from pyspark.sql.functions import explode, split
df_business_category = df_business.withColumn('category', explode(split('categories',', ')))

#Count and show the number of businesses by unique categories.
category_count = df_business_category.groupby("category").count()
category_count.show(20)


# COMMAND ----------

df_business_category.createOrReplaceTempView("category")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC category,
# MAGIC COUNT(reviews.review_id) as num_reviews
# MAGIC FROM 
# MAGIC reviews
# MAGIC JOIN category ON reviews.business_id = category.business_id 
# MAGIC GROUP BY category
# MAGIC ORDER BY num_reviews DESC;

# COMMAND ----------

# Est-ce que le « skew » des avis est négatifs?

# COMMAND ----------

#Trouver la moyenne des stars pour chaque business.
df_biz_avg_stars = df_review.groupBy('business_id').mean('stars')

#Joinde les reviews et business datasets on business_id. selectionner la moyenne avg(stars),stars,name,city,state columns.
df_joined = df_biz_avg_stars.join(df_business,'business_id')
df_selected= df_joined.select('avg(stars)','stars','name','city','state')

#Créer un nouveau dataset pour trouver le skewness de chaque ligne.
df_skewed = df_selected.select('avg(stars)','stars').toPandas()
df_skewed['skew'] = (df_skewed['avg(stars)'] - df_skewed['stars']) / df_skewed['stars']
df_skewed    

# COMMAND ----------

#Create a distribution plot.
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
 
data = df_skewed['skew']
 
sns.set_style("whitegrid")
plt.figure(figsize = (10,5)) 
sns.distplot(x = data  ,  bins = 40 , kde = True , color = 'teal'\
             , kde_kws=dict(linewidth = 1 , color = 'black'))
plt.show()
%matplot plt

# COMMAND ----------

#Calculate skewness, kurtosis, meand and variance of the dataset.
#%matplotlib inline
import numpy as np
import pandas as pd
from scipy.stats import kurtosis
from scipy.stats import skew

import matplotlib.pyplot as plt

#plt.style.use('ggplot')

data = df_skewed['skew']
np.var(data)

#plt.hist(data, bins=60)

print("mean : ", np.mean(data))
print("var  : ", np.var(data))
print("skew : ",skew(data))
print("kurt : ",kurtosis(data))

# COMMAND ----------

df_business.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM business

# COMMAND ----------

#top 10 categories by number of reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC     SELECT business_categories.*, 
# MAGIC     ROW_NUMBER() OVER (ORDER BY total_review_count DESC) rn 
# MAGIC     FROM business_categories 
# MAGIC )
# MAGIC WHERE rn <= 10

# COMMAND ----------

#Analyse top business which have over 1000 good reviews
df_business_reviews = df_business.groupBy("categories").agg(f.count("review_count").alias("total_review_count"))
df_top_business = df_business_reviews.filter(df_business_reviews["total_review_count"] >= 1000)
display(df_top_business)


# COMMAND ----------

 #Analyse number of categories available

# COMMAND ----------

# MAGIC %sql
# MAGIC Select categories,count(categories) as cat_count
# MAGIC FROM business
# MAGIC GROUP BY categories
# MAGIC ORDER BY cat_count desc
# MAGIC limit 10

# COMMAND ----------

# Find all reviews that include the word "terrible"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT text, stars, useful FROM reviews WHERE text LIKE "%terrible%"
# MAGIC LIMIT 3

# COMMAND ----------

# No. of reviews by month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT month(date) AS month, COUNT(*) AS review_count FROM reviews GROUP BY month

# COMMAND ----------

#Do user reviews become more "useful", as defined by votes of other users, the longer the user has been a member of Yelp

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT reviews.date, users.name, reviews.useful, users.yelping_since FROM reviews JOIN users ON reviews.user_id = users.user_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT users.name, COUNT(review_id) AS num_of_reviews
# MAGIC FROM reviews JOIN users ON reviews.user_id = users.user_id
# MAGIC GROUP BY reviews.user_id, users.name
# MAGIC ORDER BY COUNT(review_id) DESC
# MAGIC /*Nom de lutilisateur qui a posté le plus davis*/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT postal_code, state, AVG(business.stars) as avg_stars,
# MAGIC COUNT(business.business_id) AS num_businesses
# MAGIC FROM business
# MAGIC JOIN reviews ON business.business_id = reviews.business_id 
# MAGIC WHERE business.state = "FL"
# MAGIC GROUP BY postal_code, business.state
# MAGIC ORDER BY avg_stars DESC, num_businesses DESC
# MAGIC /*Moyenne des stars de business dans l’état de FLORIDE pour chaque code postal.*/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT U.user_id as user_id, U.useful + U.funny + U.cool as uTotal, R.useful + R.funny + R.Cool as rTotal
# MAGIC FROM users as U LEFT OUTER JOIN reviews as R
# MAGIC ON U.user_id = R.user_id
# MAGIC WHERE review_count = 1
# MAGIC /*Pour tester la relation entre les votes dans utilisateurs et ceux dans reviews*/

# COMMAND ----------

# DBTITLE 1,#Analyse de Sentiment


# COMMAND ----------

import numpy as np
import pandas as pd
import gc
import time
import warnings

# General settings
start_time = time.time()
warnings.filterwarnings("ignore")

%matplotlib inline

# COMMAND ----------

df_senti = df_review.select(["text", "stars"])
#df_senti = df_senti.na.drop()
df_senti.limit(10).toPandas()

# COMMAND ----------

# fonction pour convertir les star en scores de sentiment.
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def stars_to_sent(rating):
    if rating > 3.5:
        return 1
    elif rating < 2.5:
        return -1
    else:
        return 0

udfStarsToSent = udf(stars_to_sent, IntegerType())
df_senti = df_senti.withColumn("sentiment", udfStarsToSent("stars"))
df_senti.show(10)


# COMMAND ----------

#En raison de la taille massive de l'ensemble de données (plus de 8 millions d'avis),
#nous avons décidé de prélever un échantillon aléatoire de 20 % de l'ensemble de données d'origine.
#Les données sont ensuite divisées en ensembles d'apprentissage, de validation et de test.
#En raison du fait que l'ensemble de données est toujours aussi volumineux, nous n'avons besoin que 
#de 1 % des données pour la validation et de 1 % supplémentaire pour les tests.

# Random échantillon de 20%
df_samp = df_senti.sample(True, 0.2, seed=0)

(train_set, val_set, test_set) = df_samp.randomSplit([0.98, 0.01, 0.01], seed = 445)

print("Train: ", train_set.count())
print("Validation: ", val_set.count())
print("Test: ", test_set.count())

# COMMAND ----------

#HashingTF pour le calcul tf-idf avec régression logistique


# COMMAND ----------

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashtf = HashingTF(numFeatures=2**16, inputCol="words", outputCol="tf")
idf = IDF(inputCol="tf", outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
label_stringIdx = StringIndexer(inputCol = "sentiment", outputCol = "label")
pipeline = Pipeline(stages=[tokenizer, hashtf, idf, label_stringIdx])

pipelineFit = pipeline.fit(train_set)
train_df = pipelineFit.transform(train_set)
val_df = pipelineFit.transform(val_set)
train_df.show()

# COMMAND ----------

#Certains exemples d'avis traités sont présentés ci-dessus. Notez cependant que ce ne sont pas
#représentatif de la façon dont les valeurs tf et idf devraient ressembler en raison de la façon dont
#le DataFrame est trié (c'est-à-dire que les points d'exclamation et les guillemets viennent en premier par ordre alphabétique).

#La prochaine étape consiste à former notre modèle de régression logistique et à voir comment il se comporte pour prédire le sentiment des critiques inédites.
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(maxIter=50)
lrModel = lr.fit(train_df)
predictions = lrModel.transform(val_df)

from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prediction")
evaluator.evaluate(predictions)

# COMMAND ----------

accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(val_set.count())
accuracy

# COMMAND ----------


#CountVectorizer pour le calcul tf-idf avec régression logistique


# COMMAND ----------

# MAGIC %%time
# MAGIC from pyspark.ml.feature import CountVectorizer
# MAGIC from pyspark.ml.feature import HashingTF, IDF, Tokenizer
# MAGIC from pyspark.ml.feature import StringIndexer
# MAGIC from pyspark.ml.classification import LogisticRegression
# MAGIC from pyspark.ml import Pipeline
# MAGIC 
# MAGIC tokenizer = Tokenizer(inputCol="text", outputCol="words")
# MAGIC cv = CountVectorizer(vocabSize=2**16, inputCol="words", outputCol='cv')
# MAGIC idf = IDF(inputCol='cv', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
# MAGIC label_stringIdx = StringIndexer(inputCol = "sentiment", outputCol = "label")
# MAGIC lr = LogisticRegression(maxIter=100)
# MAGIC pipeline = Pipeline(stages=[tokenizer, cv, idf, label_stringIdx, lr])
# MAGIC 
# MAGIC from pyspark.ml.evaluation import BinaryClassificationEvaluator
# MAGIC evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prediction")
# MAGIC 
# MAGIC pipelineFit = pipeline.fit(train_set)
# MAGIC predictions = pipelineFit.transform(val_set)
# MAGIC accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(val_set.count())
# MAGIC roc_auc = evaluator.evaluate(predictions)
# MAGIC 
# MAGIC print("Accuracy Score: {0:.4f}".format(accuracy))
# MAGIC print("ROC-AUC: {0:.4f}".format(roc_auc))

# COMMAND ----------


#N-gram Implementation


# COMMAND ----------

from pyspark.ml.feature import NGram, VectorAssembler
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def build_ngrams_wocs(inputCol=["text","sentiment"], n=3):
    tokenizer = [Tokenizer(inputCol="text", outputCol="words")]
    ngrams = [
        NGram(n=i, inputCol="words", outputCol="{0}_grams".format(i))
        for i in range(1, n + 1)
    ]

    cv = [
        CountVectorizer(vocabSize=5460,inputCol="{0}_grams".format(i),
            outputCol="{0}_tf".format(i))
        for i in range(1, n + 1)
    ]
    idf = [IDF(inputCol="{0}_tf".format(i), outputCol="{0}_tfidf".format(i), minDocFreq=5) for i in range(1, n + 1)]

    assembler = [VectorAssembler(
        inputCols=["{0}_tfidf".format(i) for i in range(1, n + 1)],
        outputCol="features"
    )]
    label_stringIdx = [StringIndexer(inputCol = "sentiment", outputCol = "label")]
    lr = [LogisticRegression(maxIter=100)]
    return Pipeline(stages=tokenizer + ngrams + cv + idf+ assembler + label_stringIdx+lr)

# COMMAND ----------

# MAGIC %%time
# MAGIC 
# MAGIC from pyspark.ml.evaluation import BinaryClassificationEvaluator
# MAGIC evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prediction")
# MAGIC 
# MAGIC trigramwocs_pipelineFit = build_ngrams_wocs().fit(train_set)
# MAGIC predictions_wocs = trigramwocs_pipelineFit.transform(val_set)
# MAGIC accuracy_wocs = predictions_wocs.filter(predictions_wocs.label == predictions_wocs.prediction).count() / float(val_set.count())
# MAGIC roc_auc_wocs = evaluator.evaluate(predictions_wocs)
# MAGIC 
# MAGIC # print accuracy, roc_auc
# MAGIC print("Accuracy Score: {0:.4f}".format(accuracy_wocs))
# MAGIC print("ROC-AUC: {0:.4f}".format(roc_auc_wocs))

# COMMAND ----------


#Evaluation


# COMMAND ----------

#La dernière étape consiste à exécuter l'algorithme avec la plus grande précision sur 
#l'ensemble de test pour obtenir un résultat de précision final.

test_predictions = trigramwocs_pipelineFit.transform(test_set)
test_accuracy = test_predictions.filter(test_predictions.label == test_predictions.prediction).count() / float(test_set.count())
test_roc_auc = evaluator.evaluate(test_predictions)

# print accuracy, roc_auc
print("Accuracy Score: {0:.4f}".format(test_accuracy))
print("ROC-AUC: {0:.4f}".format(test_roc_auc))

# COMMAND ----------

#Le résultat final de précision sur l’ensemble de test est de 88%, ce qui est très bon
