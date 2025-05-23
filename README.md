# 🎵 Real-Time Amazon Music Album Reviews Analysis Pipeline

Ce projet met en place une pipeline de traitement de données en temps réel pour analyser les sentiments d'avis clients sur les Nouveaux Albums (New Releases) extraits d'Amazon Music. L'architecture utilise des microservices conteneurisés orchestrés avec Docker Compose.

---

## 🔧 Fonctionnalités

- 📦 **Scraping Amazon Music** : collecte d'avis sur les Nouveaux Albums
- 🚀 **Kafka** : transport de données entre microservices (2 brokers avec KRaft)
- 🔥 **Apache Spark** : 
  - Entraînement du modèle (Spark MLlib)
  - Prédiction en temps réel (streaming)
- 🧠 **MongoDB** : stockage des avis et sentiments prédits
- 🌐 **Backend API** : sert les données en mode temps réel et offline
- 💻 **Frontend** :
  - Statistiques en temps réel (WebSocket)
  - Historique des avis (MongoDB)
  - Export des données en mode offline

---

## 📁 Structure du projet

```
.
├── docker-compose.yml
├── spark-master/
│   ├── start_train.bat      # Entraîne le modèle une seule fois
│   └── start_service.bat    # Lance le traitement en streaming
├── kafka/
│   └── ...
├── backend/
│   └── ...
├── frontend/
│   └── ...
├── collector/
│   └── ...
└── README.md
```

---

## 🧪 Instructions d'exécution

### 🟡 Première utilisation (avec entraînement)

1. **Démarrer Spark master et workers uniquement** :
   ```bash
   docker-compose up spark-master spark-worker-1 spark-worker-2
   ```

2. **Lancer l'entraînement du modèle from local** :
   ```bash
   spark-master/start_train.bat
   ```

3. Une fois l'entraînement terminé, arrêter Spark (`Ctrl+C` ou `docker-compose down`).

4. **Lancer tous les services du projet** :
   ```bash
   docker-compose up -d
   ```

5. **Lancer le service de streaming Spark** :
   ```bash
   spark-master/start_service.bat
   ```

### 🟢 Exécutions ultérieures

1. **Démarrer tous les services directement** :
   ```bash
   docker-compose up -d
   ```

2. **Démarrer le service de prédiction Spark** :
   ```bash
   spark-master/start_service.bat
   ```

---

## 🛠️ Technologies utilisées

| Composant | Technologie |
|-----------|-------------|
| Scraping | Python, BeautifulSoup , Selenium |
| Streaming | Apache Kafka (KRaft) |
| Traitement | Apache Spark (MLlib) |
| Stockage | MongoDB |
| Backend API | FastAPI / Flask |
| Frontend | React / Vue.js |
| WebSocket | FastAPI / Socket.IO |
| Orchestration | Docker Compose |

---

## 📌 Remarques

- L'entraînement du modèle n'est requis **qu'une seule fois**.
- Le frontend propose deux modes :
  - Temps réel via WebSocket
  - Historique/Export via MongoDB
- Tu peux étendre ce projet à d'autres sites ou langages.

---

## 🧑‍💻 Auteur

**Aymane Rihane**  
[Portfolio](https://aymanerihane.github.io/myportfolio/)
