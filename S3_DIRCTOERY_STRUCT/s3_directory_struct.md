ingestion (S3 bucket) <br>
├── config/ <br>
├── data/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;├── archive/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;│&nbsp;&nbsp;&nbsp;&nbsp;└── YYYY/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── MM/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;├── in/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;├── new/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;├── out/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;├── staging/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;└── unprocessed/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── YYYY/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── MM/ <br>
├── logs/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;└── YYYY/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── MM/ <br>
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── DD/ <br>
├── ingestion-pipeline/ <br>
└── scripts/