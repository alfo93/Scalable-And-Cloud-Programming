# Progetto Scalable And Cloud Programming

Questa repository contiene il codice sorgente per l'esame relativo alla materia "Scalable and Cloud programming" effettuato nell'anno accademico 2022-23. L'obiettivo del progetto consiste nella implementazione di due algoritmi di clustering con l'obiettivo principale di valutare la fattibilità di tali algoritmi in scenari del mondo reale, concentrandoci specificamente sulla loro applicazione nel clustering delle regioni per determinare posizioni ottimali per la creazione di centri commerciali

Algoritmi implementati:
* K-means
* K-center

## Deployment su GCP

Per effettuare il deployment del progetto su Google Cloud Platofrm abbiamo eseguito i seguenti step:

* Effettuato il build del progetto mediante il tool sbt.
* Creato un bucket su GCP, nel quale abbiamo inserito il dataset interessato e l'eseguibile .jar.
* Creato un cluster su Google Dataproc.
* Eseguito multipli job per testare diverse configurazioni ed effettuare cross validation dei risultati anche successivi ad un cold boot della machina remota.

Infine, è importante notare che, prima di eseguire gli step precedentemente esposti, ci siamo assicurati che le versioni di Scala e Spark utilizzate su GCP corrispondessero a quelle utilizzate su ambienti locali.
