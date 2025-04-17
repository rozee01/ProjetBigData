# Local Vibes – Music Trends by City  
**Big Data Architecture Project**  
*Streaming + Batch Processing + Real-Time Dashboarding*  

---

## 📌 Project Overview  
**Goal**: Compare musical tastes and listening habits across cities/countries by analyzing **Twitter (streaming) + Spotify (batch) data**.  

**Key Features**:  
- Real-time heatmap of trending genres per city.  
- Identification of local artists gaining traction.  
- "Genre battle" visualization (e.g., Pop vs. Rap vs. Rock).  

---

## 🛠️ Step 1: Foundational Setup  
## 📊 Data Pipeline

Our data pipeline consists of two main flows:

### Twitter Data Flow (Streaming)
1. Capture tweets with geolocation and music-related content
2. Process through Kafka streams
3. Analyze in real-time with Spark Streaming
4. Enrich with geographical and music metadata
5. Store in time-series and data warehouse

### Spotify Data Flow (Batch)
1. Daily extraction of top charts by country
2. Process and transform with Spark batch jobs
3. Join with Twitter data for enhanced insights
4. Generate aggregated metrics and trends
5. Update dashboard visualizations
---

### 2. **Target Architecture**  
```mermaid
flowchart TD
    subgraph Data Sources
        TD[Twitter Data\nw/ Geolocation] -->|Streaming| KS[Kafka Stream]
        SD[Spotify Top Charts\nby Country] -->|Batch ETL| DL[Data Lake]
    end

    subgraph Ingestion Layer
        KS -->|Real-time Processing| SP[Spark Streaming]
        SP -->|Processed Tweets| DL
        DL -->|Batch Processing| BP[Spark Batch Processing]
    end

    subgraph Storage Layer
        BP -->|Transformed Data| DW[Data Warehouse]
        SP -->|Real-time Metrics| TS[Time Series DB]
        DW -->|Aggregated Data| RC[Redis Cache]
    end

    subgraph Analytics Layer
        DW -->|OLAP Queries| AN[Analytics Engine]
        TS -->|Time Series Analysis| AN
        AN -->|ML Models| ML[Genre/Artist Prediction]
    end

    subgraph API Layer
        RC -->|Cached Results| API[REST API]
        TS -->|Real-time Data| API
        DW -->|Historical Data| API
    end

    subgraph Presentation Layer
        API --> WA[Web Application]
        WA --> HM[Heatmap Component]
        WA --> LA[Local Artists Component]
        WA --> GB[Genre Battle Component]
    end
```  



---

### 3. **Tasks for Next Session**  
1. **Set Up Data Pipelines**:  
   - [ ] Twitter: Test API access and stream sample tweets to a Kafka topic.  
   - [ ] Spotify: Download a Kaggle dataset (e.g., [Spotify Top 200 Charts](https://www.kaggle.com/datasets/yelexa/spotify200)).  
2. **Infrastructure**:  
   - [ ] Deploy Kafka locally (Docker: `confluentinc/cp-kafka`).  
   - [ ] Prototype Spark Streaming (PySpark) to read from Kafka.  
3. **Schema Design**:  
   - Draft database tables (e.g., `tweets(ts, city, artist, genre)`, `spotify_charts(country, artist, rank)`).  

---

## 🚀 Next Steps  
- **Data Enrichment**: Use Spotify’s API to map artists to genres.  
- **Joins**: Combine batch (Spotify) + streaming (Twitter) for insights like:  
  *"In Paris, 60% of tweeted artists are local vs. 30% in global Spotify charts."*  

---

## 📚 Resources  
- [Twitter API Docs](https://developer.twitter.com/en/docs/twitter-api)  
- [Kafka + Spark Streaming Guide](https://spark.apache.org/docs/latest/streaming-kafka-integration.html)  
- [Sample Spotify Dataset](https://www.kaggle.com/datasets/yelexa/spotify200)  

---

**Team**: Arij Thabet, Mohamed Saber Azzaouzi, Mohamed Hannachi, Skander Tebourbi (13)

--- 

*Appendices*:  
- For troubleshooting Kafka, see [this guide](link).  

