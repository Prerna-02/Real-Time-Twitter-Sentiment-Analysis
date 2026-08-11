# Real-Time Twitter Sentiment Analysis

A streaming pipeline that reads tweets from a Kafka topic, classifies them using a PySpark ML model and VADER, stores results in MongoDB, and displays everything on a live Django dashboard.

The ML model is a Logistic Regression classifier trained on TF-IDF bigram features across four sentiment classes: Positive, Negative, Neutral, and Irrelevant. The dashboard updates every 5 seconds without any page refresh.


---

## Screenshots

**Live Dashboard**
![Dashboard overview](screenshots/dashboard_overview.png)

**Live Tweet Feed with sentiment labels**
![Live tweet feed](screenshots/live_feed.png)

**Entity sentiment breakdown (click any bar in the chart)**
![Entity modal](screenshots/entity_modal.png)

**Manual classify tab**
![Classify tab](screenshots/classify_tab.png)

---

## Architecture

```
Twitter CSV
    |
    v
Producer (producer.py)
    |  publishes JSON to Kafka topic "tweets"
    v
Kafka Broker (Docker)
    |
    v
Consumer (consumer.py)
    |  PySpark Structured Streaming
    |  - Spark ML inference (LR + TF-IDF bigrams)
    |  - VADER scoring
    |  - writes results to MongoDB
    v
MongoDB (twitter_sentiment.tweets)
    |
    v
Django Dashboard (127.0.0.1:8000)
    |  - Live tweet table
    |  - Sentiment pie chart
    |  - Top entities bar chart
    |  - Manual classify tab
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Message broker | Apache Kafka + Zookeeper (Docker) |
| Stream processing | PySpark 3.5 Structured Streaming |
| ML model | Logistic Regression with TF-IDF bigrams (Spark MLlib) |
| Rule-based NLP | VADER SentimentIntensityAnalyzer |
| Database | MongoDB |
| Dashboard | Django 5 + Chart.js + Bootstrap 5 |
| Environment | Conda (Python 3.11, OpenJDK 11) |

---

## Project Structure

```
Real-Time-Twitter-Sentiment-Analysis/
├── Django-Dashboard/
│   ├── manage.py
│   ├── dashboard/
│   │   ├── views.py          # API endpoints + Spark inference
│   │   ├── models.py         # MongoDB connection
│   │   ├── templates/        # dashboard.html, classify.html, base.html
│   │   └── static/           # dashboard.js, style.css
│   └── sentimentapp/
│       └── settings.py       # Mongo URI, Spark model path
├── Kafka-PySpark/
│   ├── producer.py           # reads CSV, publishes to Kafka
│   ├── consumer.py           # Spark streaming, writes to MongoDB
│   └── config.py             # shared constants
├── ML-PySpark-Model/
│   ├── datasets/             # twitter_training.csv, twitter_validation.csv
│   └── notebooks/
│       ├── train_model.ipynb # 6-stage pipeline training + evaluation
│       └── data_exploration.ipynb
├── environment.yml
├── requirements.txt
└── zk-single-kafka-single.yml
```

---

## Prerequisites

- Windows 10/11 with [Anaconda](https://www.anaconda.com/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [MongoDB Community](https://www.mongodb.com/try/download/community) running on localhost:27017
- Hadoop winutils for Windows: place `winutils.exe` in `C:\hadoop\bin` and set `HADOOP_HOME=C:\hadoop`

---

## Setup

**1. Create the Conda environment**
```bash
conda env create -f environment.yml
conda activate sentiment_env
```

**2. Train the ML model** (one time only, takes 3-5 minutes)

Open `ML-PySpark-Model/notebooks/train_model.ipynb` and run all cells. This saves the trained pipeline to `ML-PySpark-Model/saved_models/spark_lr_pipeline`.

**3. Start Kafka**
```bash
docker-compose -f zk-single-kafka-single.yml up -d
```

---

## Running the Pipeline

Open three separate terminals, all with `conda activate sentiment_env`.

**Terminal 1 - Spark consumer**
```bash
cd Kafka-PySpark
python consumer.py
```

**Terminal 2 - Kafka producer**
```bash
cd Kafka-PySpark
python producer.py
```

**Terminal 3 - Django dashboard**
```bash
cd Django-Dashboard
python manage.py migrate
python manage.py runserver
```

Open http://127.0.0.1:8000 to see the live dashboard.

---

## Classify Tab

The classify tab lets you type any tweet and get instant predictions from both models. VADER responds immediately. Spark ML takes about 30 seconds on the first request (JVM cold start) and is instant after that.

Note: the classify tab uses its own SparkSession. Stop consumer.py before using it, since only one SparkSession can run at a time on a single machine.

---

## Dataset

The project uses the [Twitter Entity Sentiment Analysis](https://www.kaggle.com/datasets/jp797498e/twitter-entity-sentiment-analysis) dataset from Kaggle. Place the CSV files in `ML-PySpark-Model/datasets/` before training.

| File | Rows |
|---|---|
| twitter_training.csv | 73,996 |
| twitter_validation.csv | 1,000 |

Four classes: Positive, Negative, Neutral, Irrelevant.

---

## Model Performance

Evaluated on the 1,000-row validation set. All figures are reproducible by running section 9 of `train_model.ipynb`.

Because the classes are imbalanced and one of them (**Irrelevant**, 17.2% of the set) is a class VADER can never predict, results are reported two ways: the full 4-class task, and a 3-class slice with the Irrelevant rows removed to give VADER a fair comparison.

**4-class — the real task (n=1000)**

| Model | Accuracy | Macro F1 | Weighted F1 |
|---|---|---|---|
| Spark ML (LR + TF-IDF bigrams) | **86.1%** | **0.861** | **0.861** |
| VADER | 40.0% | 0.307 | 0.337 |

**3-class — Irrelevant excluded, VADER's home turf (n=828)**

| Model | Accuracy | Macro F1 | Weighted F1 |
|---|---|---|---|
| Spark ML (LR + TF-IDF bigrams) | **86.5%** | **0.876** | **0.876** |
| VADER | 48.3% | 0.454 | 0.451 |

**Per-class breakdown — Spark ML, 4-class**

| Class | Precision | Recall | F1 | Support |
|---|---|---|---|---|
| Negative | 0.907 | 0.883 | 0.895 | 266 |
| Neutral | 0.892 | 0.811 | 0.849 | 285 |
| Positive | 0.791 | 0.903 | 0.843 | 277 |
| Irrelevant | 0.873 | 0.843 | 0.858 | 172 |

**Per-class breakdown — VADER, 4-class**

| Class | Precision | Recall | F1 | Support |
|---|---|---|---|---|
| Negative | 0.399 | 0.579 | 0.472 | 266 |
| Neutral | 0.362 | 0.175 | 0.236 | 285 |
| Positive | 0.412 | 0.708 | 0.521 | 277 |
| Irrelevant | 0.000 | 0.000 | 0.000 | 172 |

**Model agreement**

The two models agree on only 38.0% of tweets. When they agree, they are right 90.3% of the time — making agreement a useful confidence signal. When they disagree, Spark is correct 83.5% of the time versus VADER's 9.2%, which is why the pipeline treats Spark as the primary label and VADER as a secondary indicator.

**Notes on methodology**

- VADER is scored on the **raw** tweet text while Spark is scored on the cleaned text. VADER derives intensity from punctuation, capitalisation and emoji, all of which `clean_text()` strips — scoring it on cleaned text would understate it. This matches the live pipeline, where `consumer.py` runs VADER on `original_text`.
- Dropping the Irrelevant class lifts VADER from 40.0% to 48.3%, confirming part of its 4-class score was an artefact of being asked a question it cannot answer. It remains ~38 points behind Spark even on the fair comparison.
- VADER's Neutral recall (0.175) is its weakest result: it only reports Neutral when a tweet has almost no charged vocabulary, whereas annotators label by intent, so factual or informational tweets with charged words get misread as polarised.
- With n=1000, accuracy near 86% carries roughly ±2.1 points at 95% confidence. The gaps above are far larger than that margin, but 1–2 point differences should not be over-interpreted.
