import sys
import os

# Must be set before any PySpark import so workers use the same Python as Django
os.environ.setdefault('PYSPARK_PYTHON',        sys.executable)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)
os.environ.setdefault('HADOOP_HOME', r'C:\hadoop')
_hadoop_bin = r'C:\hadoop\bin'
if _hadoop_bin not in os.environ.get('PATH', ''):
    os.environ['PATH'] = _hadoop_bin + os.pathsep + os.environ.get('PATH', '')

from django.shortcuts import render
from django.http import JsonResponse
from django.conf import settings
from django.views.decorators.http import require_POST

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from .models import collection

_vader = SentimentIntensityAnalyzer()

# Spark is loaded lazily on the first /classify/ POST (JVM startup ~30 s).
_spark       = None
_spark_model = None


def _get_spark_model():
    global _spark, _spark_model
    if _spark_model is None:
        from pyspark.sql import SparkSession
        from pyspark.ml import PipelineModel

        _spark = (
            SparkSession.builder
            .appName("DjangoSentimentClassify")
            .config("spark.driver.memory",          "2g")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.local.dir",              "C:/tmp/spark")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")

        model_path = settings.SPARK_MODEL_PATH.replace('\\', '/')
        _spark_model = PipelineModel.load(model_path)

    return _spark, _spark_model


# Page views

def dashboard(request):
    return render(request, 'dashboard.html')


def classify(request):
    global _spark, _spark_model
    if request.method != 'POST':
        return render(request, 'classify.html')

    text = request.POST.get('text', '').strip()
    if not text:
        return JsonResponse({'error': 'No text provided.'}, status=400)

    # VADER (always fastno JVM required)
    scores   = _vader.polarity_scores(text)
    compound = scores['compound']
    vader_label = (
        'Positive' if compound >= 0.05
        else 'Negative' if compound <= -0.05
        else 'Neutral'
    )

    # Spark ML inference
    spark_label  = 'Unavailable'
    confidence   = 0.0
    spark_ok     = False
    spark_error  = ''

    try:
        from pyspark.sql import Row
        from pyspark.sql.functions import regexp_replace, lower, trim, col

        spark, model = _get_spark_model()

        df = spark.createDataFrame([Row(text=text)])
        df = (
            df
            .withColumn('text', regexp_replace(col('text'), r'http\S+',     ''))
            .withColumn('text', regexp_replace(col('text'), r'@\w+',        ''))
            .withColumn('text', regexp_replace(col('text'), r'[^a-zA-Z\s]', ''))
            .withColumn('text', trim(lower(col('text'))))
        )

        # Stage 0 is StringIndexer (needs 'sentiment' column, training only).
        # Apply stages 1+ for inference.
        result_df = df
        for stage in model.stages[1:]:
            result_df = stage.transform(result_df)
        result      = result_df.collect()[0]
        labels      = model.stages[0].labels
        spark_label = labels[int(result.prediction)]
        confidence  = float(max(result.probability))
        spark_ok    = True

    except Exception as exc:
        spark_error = str(exc)
        _spark = None
        _spark_model = None

    return JsonResponse({
        'vader': {
            'label':    vader_label,
            'compound': round(compound, 4),
        },
        'spark': {
            'label':      spark_label,
            'confidence': round(confidence * 100, 1),
            'ok':         spark_ok,
            'error':      spark_error,
        },
    })


#  API endpoints (called by dashboard.js every 5 s)

def api_tweets(request):
    entity = request.GET.get('entity', '').strip()
    limit  = min(int(request.GET.get('limit', 50)), 200)

    query = {}
    if entity:
        query['entity'] = entity

    docs = list(
        collection
        .find(query, {'_id': 0})
        .sort('_id', -1)
        .limit(limit)
    )
    return JsonResponse({'tweets': docs})


def api_stats(request):
    # Optional entity filterif present, all stats except the global
    # entity-volume bar chart are restricted to that entity.
    entity = request.GET.get('entity', '').strip()
    entity_filter = {'entity': entity} if entity else {}
    match_stage = [{'$match': entity_filter}] if entity else []

    total = collection.count_documents(entity_filter)

    if total == 0:
        return JsonResponse({
            'total':              0,
            'positive_pct':       0,
            'negative_pct':       0,
            'spark_distribution': [],
            'vader_distribution': [],
            'top_entities':       [],
            'entities':           [],
            'selected_entity':    entity,
        })

    # Sentiment distributions (filtered if entity selected)
    spark_dist = list(collection.aggregate(
        match_stage + [
            {'$group': {'_id': '$sentiment_spark', 'count': {'$sum': 1}}},
            {'$sort':  {'count': -1}},
        ]
    ))
    vader_dist = list(collection.aggregate(
        match_stage + [
            {'$group': {'_id': '$sentiment_vader', 'count': {'$sum': 1}}},
            {'$sort':  {'count': -1}},
        ]
    ))

    # Top 10 entitiesALWAYS global, never filtered (it's the navigation aid)
    top_entities = list(collection.aggregate([
        {'$group': {'_id': '$entity', 'count': {'$sum': 1}}},
        {'$sort':  {'count': -1}},
        {'$limit': 10},
    ]))

    # Positive / Negative % from Spark distribution (the primary model)
    spark_dist_map = {d['_id']: d['count'] for d in spark_dist if d['_id']}
    positive_pct = round(spark_dist_map.get('Positive', 0) / total * 100, 1)
    negative_pct = round(spark_dist_map.get('Negative', 0) / total * 100, 1)

    # Full entity listglobal, populates the dropdown
    entities = sorted([e for e in collection.distinct('entity') if e])

    return JsonResponse({
        'total':              total,
        'positive_pct':       positive_pct,
        'negative_pct':       negative_pct,
        'spark_distribution': [{'label': d['_id'], 'count': d['count']} for d in spark_dist if d['_id']],
        'vader_distribution': [{'label': d['_id'], 'count': d['count']} for d in vader_dist if d['_id']],
        'top_entities':       [{'entity': d['_id'], 'count': d['count']} for d in top_entities if d['_id']],
        'entities':           entities,
        'selected_entity':    entity,
    })