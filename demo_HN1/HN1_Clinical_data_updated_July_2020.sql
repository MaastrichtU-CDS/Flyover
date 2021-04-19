CREATE TABLE "HN1_Clinical_data_updated_July_2020" (
    "id" TEXT,
    "index_tumour_location" TEXT,
    "age_at_diagnosis" INT,
    "biological_sex" TEXT,
    "performance_status_ecog" INT,
    "overall_hpv_p16_status" TEXT,
    "clin_t" INT,
    "clin_n" INT,
    "clin_m" INT,
    "ajcc_stage" TEXT,
    "pretreat_hb_in_mmolperlitre" NUMERIC(3, 1),
    "cancer_surgery_performed" TEXT,
    "chemotherapy_given" TEXT,
    "radiotherapy_total_treat_time" INT,
    "radiotherapy_refgydose_perfraction_highriskgtv" NUMERIC(2, 1),
    "radiotherapy_refgydose_total_highriskgtv" NUMERIC(3, 1),
    "radiotherapy_number_fractions_highriskgtv" INT,
    "event_overall_survival" INT,
    "overall_survival_in_days" INT,
    "event_recurrence_metastatic_free_survival" INT,
    "recurrence_metastatic_free_survival_in_days" INT,
    "event_local_recurrence" INT,
    "local_recurrence_in_days" INT,
    "event_locoregional_recurrence" INT,
    "locoregional_recurrence_in_days" INT,
    "event_distant_metastases" INT,
    "distant_metastases_in_days" INT
);
INSERT INTO "HN1_Clinical_data_updated_July_2020" VALUES
    ('HN1004','oropharynx',56,'male',1,'negative',4,2,0,'iva',7.1,'no','concomitant',46,2,70,35,1,3193,0,3193,0,3193,0,3193,0,3193),
    ('HN1006','oropharynx',63,'female',1,'negative',1,2,0,'iva',NULL,'no','none',37,2,68,34,0,2805,0,1940,0,1940,0,1940,0,1940),
    ('HN1022','oropharynx',56,'male',0,'positive',1,0,0,'i',NULL,'no','none',37,2,68,34,0,3198,0,1269,0,1269,0,1269,0,1269),
    ('HN1026','larynx',67,'male',0,NULL,4,0,0,'iva',9.3,'no','none',36,2,68,34,0,3244,1,600,1,600,1,600,0,1315),
    ('HN1029','oropharynx',54,'female',0,'positive',1,2,0,'iva',7.4,'no','none',37,2,68,34,0,2925,0,2101,0,2101,0,2101,0,2101),
    ('HN1046','oropharynx',66,'male',0,'negative',2,2,0,'iva',NULL,'no','none',37,2,68,34,1,828,1,707,0,749,0,749,1,707),
    ('HN1047','larynx',73,'male',0,NULL,1,0,0,'i',NULL,'no','none',32,2.4,60,25,0,3205,0,1276,0,1276,0,1276,0,1276),
    ('HN1054','oropharynx',64,'male',0,'positive',2,0,0,'ii',8.2,'no','none',37,2,68,34,0,2978,0,1049,0,1049,0,1049,0,1049),
    ('HN1057','oropharynx',53,'female',1,NULL,2,1,0,'iii',NULL,'no','none',37,2,68,34,1,989,1,462,1,462,1,462,0,847),
    ('HN1060','oropharynx',44,'male',NULL,'negative',4,2,0,'iva',8.5,'no','none',37,2,68,34,1,1173,1,707,0,1159,0,1159,1,707),
    ('HN1062','larynx',54,'male',1,NULL,1,0,0,'i',NULL,'no','none',36,2,68,34,1,1229,0,883,0,883,0,883,0,883),
    ('HN1067','oropharynx',69,'male',1,'negative',3,0,0,'iii',8.9,'no','concurrent',45,2,70,35,1,3927,0,2318,0,2318,0,2318,0,2318),
    ('HN1074','larynx',76,'male',NULL,NULL,1,1,0,'iii',NULL,'no','none',37,2,68,34,1,1755,0,1755,0,1755,0,1755,0,1755),
    ('HN1077','larynx',82,'male',0,NULL,1,0,0,'i',NULL,'no','none',35,2.4,60,25,1,1164,0,1002,0,1002,0,1002,0,1002),
    ('HN1079','oropharynx',71,'male',1,'positive',3,2,0,'iva',8.7,'no','none',37,2,68,34,1,743,1,532,0,578,0,578,1,532),
    ('HN1080','oropharynx',70,'male',1,'negative',2,1,0,'iii',NULL,'no','none',37,2,68,34,0,3044,0,1869,0,1869,0,1869,0,1869),
    ('HN1081','oropharynx',55,'male',1,'negative',1,0,0,'i',NULL,'no','none',37,2,68,34,0,3128,0,1199,0,1199,0,1199,0,1199),
    ('HN1083','larynx',70,'male',0,NULL,1,0,0,'i',NULL,'no','none',44,2,66,33,0,3128,0,1199,0,1199,0,1199,0,1199),
    ('HN1088','oropharynx',46,'female',0,'positive',4,2,0,'iva',8.9,'no','concomitant',56,2,70,35,0,4045,0,2116,0,2116,0,2116,0,2116),
    ('HN1092','oropharynx',77,'female',NULL,'negative',2,1,0,'iii',NULL,'no','none',36,2,68,34,1,355,1,132,1,132,1,132,0,197),
    ('HN1095','larynx',62,'male',2,NULL,4,3,0,'ivb',8.2,'no','none',36,2,68,34,1,398,1,317,1,317,1,317,0,325),
    ('HN1096','oropharynx',54,'male',0,'negative',4,2,0,'iva',9,'no','concurrent',46,2,70,35,1,3925,0,2436,0,2436,0,2436,0,2436),
    ('HN1102','oropharynx',60,'male',0,'negative',3,2,0,'iva',7.8,'no','concurrent',46,2,70,35,1,103,0,102,0,102,0,102,0,102),
    ('HN1106','larynx',80,'male',0,NULL,1,0,0,'i',NULL,'no','none',31,2.4,60,25,1,2288,0,837,0,837,0,837,0,837),
    ('HN1117','oropharynx',67,'male',0,'positive',4,2,0,'ivb',8.3,'no','none',37,2,68,34,1,1668,0,1668,0,1668,0,1668,0,1668),
    ('HN1118','larynx',55,'male',1,NULL,4,0,0,'iva',NULL,'no','none',37,2,68,34,0,2778,0,849,0,849,0,849,0,849),
    ('HN1123','larynx',60,'male',1,NULL,4,0,0,'iva',NULL,'no','none',37,2,68,34,0,3177,0,1248,0,1248,0,1248,0,1248),
    ('HN1127','larynx',54,'male',0,NULL,1,2,0,'iva',NULL,'yes','none',37,2,68,34,1,2834,1,1331,0,2834,1,1331,0,2834),
    ('HN1135','oropharynx',51,'male',1,'negative',3,1,0,'iii',9.8,'no','concomitant',45,2,70,35,1,448,1,322,0,430,1,322,0,430),
    ('HN1139','oropharynx',64,'female',1,NULL,2,0,0,'ii',NULL,'no','none',37,2,68,34,0,2798,1,146,0,869,1,146,0,869),
    ('HN1146','oropharynx',65,'female',1,'negative',2,0,0,'ii',7.8,'no','none',37,2,68,34,1,2056,0,1577,0,1577,0,1577,0,1577),
    ('HN1159','oropharynx',50,'female',1,NULL,4,2,0,'ivb',8.7,'no','concomitant',45,2,70,35,0,3904,0,1975,0,1975,0,1975,0,1975),
    ('HN1170','larynx',57,'male',0,NULL,3,0,0,'iii',9.3,'no','none',38,2,68,34,0,3093,0,1347,0,1347,0,1347,0,1347),
    ('HN1175','larynx',74,'male',1,NULL,3,2,1,'ivc',9.4,'no','none',36,2,68,34,1,194,1,151,0,162,1,151,0,162),
    ('HN1180','oropharynx',76,'male',1,'negative',4,2,0,'iva',8.1,'no','none',46,2,70,35,1,599,0,478,0,478,0,478,0,478),
    ('HN1192','oropharynx',60,'male',NULL,'negative',3,0,0,'iii',8.1,'no','none',36,2,68,34,1,511,0,466,0,466,0,466,0,466),
    ('HN1197','oropharynx',54,'male',1,'negative',3,1,0,'iii',8.7,'no','concurrent',46,2,70,35,0,3351,0,1429,0,1429,0,1429,0,1429),
    ('HN1200','oropharynx',56,'male',0,'negative',1,0,0,'i',NULL,'no','none',36,2,68,34,0,4643,0,2718,0,2718,0,2718,0,2718),
    ('HN1201','oropharynx',59,'male',0,'positive',1,0,0,'i',NULL,'no','none',37,2,68,34,0,2925,0,1040,0,1040,0,1040,0,1040),
    ('HN1208','larynx',61,'male',1,NULL,4,2,0,'iva',5,'no','none',38,2,68,34,1,55,1,38,1,38,1,38,0,55),
    ('HN1215','oropharynx',54,'male',1,'positive',2,2,0,'iva',9.1,'no','concomitant',47,2,70,35,0,2831,0,1793,0,1793,0,1793,0,1793),
    ('HN1244','oropharynx',65,'male',1,'negative',2,1,0,'iii',NULL,'no','none',39,2,68,34,1,2599,0,2025,0,2025,0,2025,0,2025),
    ('HN1259','oropharynx',69,'female',0,'negative',3,2,0,'iva',NULL,'no','none',37,2,68,34,1,2331,0,1783,0,1783,0,1783,0,1783),
    ('HN1260','oropharynx',47,'male',NULL,'negative',3,2,0,'iva',NULL,'no','none',37,2,68,34,0,4734,0,2809,0,2809,0,2809,0,2809),
    ('HN1263','oropharynx',69,'male',NULL,'negative',1,0,0,'i',NULL,'no','none',36,2,68,34,1,1310,0,1016,0,1016,0,1016,0,1016),
    ('HN1271','oropharynx',65,'male',1,'positive',2,0,0,'ii',NULL,'no','none',44,2,68,34,0,3918,0,1989,0,1989,0,1989,0,1989),
    ('HN1280','larynx',71,'male',1,NULL,1,0,0,'i',NULL,'no','none',34,2.4,60,25,0,2846,0,811,0,811,0,811,0,811),
    ('HN1294','oropharynx',50,'male',0,'negative',2,2,0,'iva',8.7,'no','concomitant',46,2,70,35,0,2799,0,2107,0,2107,0,2107,0,2107),
    ('HN1305','oropharynx',60,'male',1,'negative',2,1,0,'iii',8.8,'no','none',36,2,68,34,0,3254,1,3200,1,3200,1,3200,0,3254),
    ('HN1308','oropharynx',57,'female',0,NULL,2,2,0,'iva',7.6,'no','concomitant',46,2,70,35,1,731,0,718,0,718,0,718,0,718),
    ('HN1310','larynx',57,'male',1,NULL,2,0,0,'ii',NULL,'no','none',44,2,66,33,0,2972,0,980,0,980,0,980,0,980),
    ('HN1319','oropharynx',46,'male',1,'negative',4,2,0,'iva',11.3,'no','none',37,2,68,34,1,595,0,588,0,588,0,588,0,588),
    ('HN1323','oropharynx',61,'male',0,'negative',4,2,0,'iva',8.8,'no','concurrent',47,2,70,35,1,912,0,827,0,827,0,827,0,827),
    ('HN1324','larynx',50,'female',0,NULL,4,0,0,'iva',8,'no','none',37,2,68,34,1,1998,0,1998,0,1998,0,1998,0,1998),
    ('HN1327','larynx',58,'male',0,NULL,1,0,0,'i',9.7,'no','none',44,2,66,33,0,3247,0,1318,0,1318,0,1318,0,1318),
    ('HN1331','oropharynx',46,'male',NULL,NULL,1,0,0,'i',9.7,'no','none',36,2,68,34,0,4789,0,2869,0,2869,0,2869,0,2869),
    ('HN1339','oropharynx',54,'male',0,'negative',4,0,0,'iva',6.3,'no','none',38,2,68,34,1,88,1,38,1,38,1,38,0,81),
    ('HN1342','larynx',79,'male',0,NULL,1,0,0,'i',8.7,'no','none',34,2.4,60,25,1,2860,0,945,0,945,0,945,0,945),
    ('HN1344','oropharynx',55,'male',1,'negative',4,2,0,'iva',9.6,'yes','concomitant',46,2,70,35,1,1352,1,268,0,1352,1,268,0,1352),
    ('HN1355','larynx',64,'male',1,NULL,2,0,0,'ii',NULL,'no','none',38,2,68,34,0,3093,1,358,1,358,1,358,0,1164),
    ('HN1356','oropharynx',66,'male',0,'negative',3,2,0,'iva',8.1,'no','none',37,2,68,34,1,1538,1,724,1,724,1,724,1,1197),
    ('HN1357','larynx',70,'male',1,NULL,4,0,0,'iva',8.9,'yes','none',37,2,68,34,0,2960,1,393,1,393,1,393,0,2867),
    ('HN1363','larynx',74,'male',0,NULL,4,2,0,'ivb',9.4,'no','none',37,2,68,34,1,2563,0,2563,0,2563,0,2563,0,2563),
    ('HN1367','oropharynx',63,'male',0,'negative',4,2,0,'iva',8.6,'no','concomitant',36,2,68,34,1,541,0,536,0,536,0,536,0,536),
    ('HN1368','oropharynx',69,'female',0,'positive',1,2,0,'iva',NULL,'no','none',37,2,68,34,0,3247,0,2013,0,2013,0,2013,0,2013),
    ('HN1369','oropharynx',57,'male',NULL,'negative',4,2,0,'iva',8.1,'no','concomitant',45,2,70,35,1,1172,0,1142,0,1142,0,1142,0,1142),
    ('HN1371','larynx',72,'male',1,NULL,3,0,0,'iii',8.3,'no','none',38,2,68,34,1,200,0,200,0,200,0,200,0,200),
    ('HN1372','oropharynx',54,'male',0,'positive',4,0,0,'iva',9.1,'no','concurrent',46,2,70,35,0,1411,0,1411,0,1411,0,1411,0,1411),
    ('HN1395','oropharynx',56,'male',0,'positive',4,0,0,'iva',10,'no','concomitant',39,2,68,34,0,3667,0,1738,0,1738,0,1738,0,1738),
    ('HN1400','oropharynx',56,'male',1,'negative',4,1,0,'iva',10,'no','concomitant',46,2,70,35,1,2545,0,1598,0,1598,0,1598,0,1598),
    ('HN1412','oropharynx',53,'female',0,'negative',3,2,0,'iva',NULL,'no','concurrent',39,2,68,34,1,2525,0,1796,0,1796,0,1796,0,1796),
    ('HN1417','oropharynx',60,'male',2,'negative',4,0,0,'iva',6.2,'no','concurrent',45,2,70,35,0,3218,0,1289,0,1289,0,1289,0,1289),
    ('HN1429','oropharynx',62,'male',1,'negative',2,2,0,'iva',8.5,'no','concomitant',46,2,70,35,1,298,1,196,1,196,1,196,1,196),
    ('HN1442','larynx',61,'male',0,NULL,4,2,0,'iva',9.2,'no','none',37,2,68,34,1,179,1,147,1,147,1,147,0,179),
    ('HN1444','larynx',66,'male',0,NULL,1,0,0,'i',NULL,'no','none',43,2,66,33,0,2796,0,876,0,876,0,876,0,876),
    ('HN1461','oropharynx',80,'male',0,NULL,4,2,0,'iva',NULL,'no','none',37,2,68,34,1,220,1,174,1,174,1,174,0,205),
    ('HN1465','larynx',61,'male',0,NULL,3,0,0,'iii',5.8,'no','none',37,2,68,34,0,3275,1,1051,1,1051,1,1051,0,3111),
    ('HN1469','oropharynx',59,'male',0,'negative',2,2,0,'iva',9.8,'no','concomitant',47,2,70,35,1,1594,0,1594,0,1594,0,1594,0,1594),
    ('HN1483','oropharynx',51,'female',0,'positive',2,2,0,'iva',6.8,'no','concomitant',45,2,70,35,0,4572,0,2647,0,2647,0,2647,0,2647),
    ('HN1485','larynx',70,'female',0,NULL,2,0,0,'ii',NULL,'no','none',36,2,68,34,0,1023,1,669,0,1023,1,669,0,1023),
    ('HN1486','larynx',70,'male',1,NULL,3,2,0,'iva',7.2,'no','none',37,2,68,34,1,461,0,461,0,461,0,461,0,461),
    ('HN1487','oropharynx',62,'male',1,'negative',4,2,0,'iva',8,'no','concomitant',48,2,70,35,1,858,1,697,1,697,1,697,0,697),
    ('HN1488','oropharynx',57,'male',1,'positive',4,2,0,'ivb',8.8,'no','concurrent',46,2,70,35,0,3471,0,1542,0,1542,0,1542,0,1542),
    ('HN1491','oropharynx',58,'male',0,'negative',4,1,0,'iva',10.2,'no','concurrent',46,2,70,35,0,3632,0,1703,0,1703,0,1703,0,1703),
    ('HN1500','larynx',54,'female',1,NULL,4,0,0,'iva',7.6,'no','none',37,2,68,34,1,2505,0,912,0,912,0,912,0,912),
    ('HN1501','oropharynx',49,'male',2,'positive',4,2,0,'iva',NULL,'no','none',46,2,70,35,1,1171,0,1171,0,1171,0,1171,0,1171),
    ('HN1502','larynx',76,'male',1,NULL,3,0,0,'iii',NULL,'no','none',36,1.8,50.4,28,1,48,1,36,1,36,1,36,0,42),
    ('HN1514','larynx',83,'male',0,NULL,1,0,0,'i',8.6,'no','none',32,2.4,60,25,0,3023,0,1094,0,1094,0,1094,0,1094),
    ('HN1517','oropharynx',66,'male',0,'negative',2,0,0,'ii',9.2,'no','none',36,2,68,34,1,1753,0,848,0,848,0,848,0,848),
    ('HN1519','oropharynx',56,'female',0,'negative',4,2,0,'iva',7.7,'no','concomitant',48,2,70,35,1,475,1,285,1,285,1,285,1,285),
    ('HN1524','larynx',77,'male',1,NULL,1,0,0,'i',NULL,'no','none',34,2.4,60,25,1,931,0,923,0,923,0,923,0,923),
    ('HN1538','larynx',67,'female',0,NULL,4,1,0,'iva',NULL,'no','none',35,2,68,34,1,2455,1,539,1,539,1,539,0,882),
    ('HN1549','oropharynx',57,'female',0,'negative',1,2,0,'iva',8.6,'no','none',37,2,68,34,1,485,0,485,0,485,0,485,0,485),
    ('HN1554','larynx',62,'male',0,NULL,1,0,0,'i',NULL,'no','none',44,2,66,33,0,3365,0,1436,0,1436,0,1436,0,1436),
    ('HN1555','oropharynx',51,'male',0,'negative',4,0,0,'iva',7.9,'no','concomitant',46,2,70,35,0,2813,0,2184,0,2184,0,2184,0,2184),
    ('HN1560','oropharynx',58,'male',1,'positive',2,2,0,'iva',NULL,'no','none',37,2,68,34,0,4157,0,2228,0,2228,0,2228,0,2228),
    ('HN1562','larynx',70,'male',0,NULL,1,0,0,'i',NULL,'no','none',33,2.4,60,25,0,2974,0,990,0,990,0,990,0,990),
    ('HN1572','oropharynx',53,'female',1,'negative',4,2,0,'iva',7.6,'no','concomitant',50,2,70,35,0,4307,0,2382,0,2382,0,2382,0,2382),
    ('HN1600','larynx',63,'male',1,NULL,1,0,0,'i',NULL,'no','none',44,2,66,33,0,3366,0,1437,0,1437,0,1437,0,1437),
    ('HN1609','oropharynx',63,'female',0,'negative',2,0,0,'ii',8.1,'no','none',35,2,68,34,0,3952,0,2081,0,2081,0,2081,0,2081),
    ('HN1610','larynx',63,'male',1,NULL,3,2,0,'iva',NULL,'no','none',37,2,68,34,1,2614,0,2614,0,2614,0,2614,0,2614),
    ('HN1640','oropharynx',61,'female',0,'positive',3,2,0,'iva',7.9,'no','concomitant',45,2,70,35,0,2826,0,2292,0,2292,0,2292,0,2292),
    ('HN1648','oropharynx',63,'male',1,'negative',4,2,0,'iva',5.6,'no','none',37,2,68,34,1,422,1,357,0,360,0,360,1,357),
    ('HN1653','larynx',56,'male',1,NULL,3,0,0,'iii',NULL,'no','none',37,2,68,34,1,264,1,255,1,255,1,255,0,263),
    ('HN1667','larynx',79,'male',1,NULL,1,0,0,'i',9.5,'no','none',34,2.4,60,25,1,2079,0,1303,0,1303,0,1303,0,1303),
    ('HN1679','larynx',67,'male',1,NULL,1,0,0,'i',NULL,'no','none',46,2,66,33,0,3351,0,1422,0,1422,0,1422,0,1422),
    ('HN1697','oropharynx',55,'male',1,NULL,3,0,0,'iii',8.5,'no','concomitant',46,2,70,35,0,4164,0,2235,0,2235,0,2235,0,2235),
    ('HN1703','oropharynx',60,'male',1,'negative',4,0,0,'iva',7.3,'no','concomitant',46,2,70,35,1,154,0,137,0,137,0,137,0,137),
    ('HN1719','oropharynx',56,'female',0,'negative',1,2,0,'iva',8.4,'yes','concurrent',47,2,70,35,0,3478,0,1549,0,1549,0,1549,0,1549),
    ('HN1748','oropharynx',58,'male',NULL,'positive',2,2,0,'iva',NULL,'no','none',36,2,68,34,0,2910,0,2910,0,2910,0,2910,0,2910),
    ('HN1760','larynx',67,'male',0,NULL,1,0,0,'i',9.9,'no','none',44,2,66,33,0,2846,1,426,1,426,1,426,0,917),
    ('HN1791','oropharynx',64,'male',1,'negative',1,1,0,'iii',NULL,'no','none',36,2,68,34,1,1444,0,890,0,890,0,890,0,890),
    ('HN1792','oropharynx',60,'male',1,'negative',4,2,0,'ivb',9,'no','none',48,2,70,35,1,278,1,48,1,48,1,48,0,250),
    ('HN1793','larynx',61,'male',1,'negative',2,2,0,'iva',8.9,'no','none',36,2,68,34,1,424,1,400,0,400,0,400,1,400),
    ('HN1805','oropharynx',52,'male',1,'positive',4,2,0,'ivb',9.8,'yes','concomitant',43,2,70,35,0,3902,0,1973,0,1973,0,1973,0,1973),
    ('HN1813','oropharynx',74,'male',0,'positive',4,1,0,'iva',7.8,'no','none',37,2,68,34,1,3084,0,1572,0,1572,0,1572,0,1572),
    ('HN1815','larynx',66,'male',0,NULL,3,0,0,'iii',NULL,'no','none',37,2,68,34,1,1550,0,1024,0,1024,0,1024,0,1024),
    ('HN1827','oropharynx',68,'male',1,'negative',1,2,0,'iva',9.1,'no','none',45,2,70,35,1,468,1,45,0,388,1,45,0,388),
    ('HN1838','oropharynx',62,'male',1,'negative',2,3,0,'ivb',9.2,'no','none',37,2,68,34,1,3037,0,2801,0,2801,0,2801,0,2801),
    ('HN1839','oropharynx',65,'male',3,'negative',4,2,0,'iva',NULL,'no','none',37,2,68,34,1,281,0,161,0,161,0,161,0,161),
    ('HN1851','oropharynx',60,'male',0,'negative',2,2,0,'iva',8.5,'no','concomitant',46,2,70,35,0,2925,0,1869,0,1869,0,1869,0,1869),
    ('HN1860','larynx',76,'male',1,NULL,1,0,0,'i',NULL,'no','none',35,2.4,60,25,0,2733,0,864,0,864,0,864,0,864),
    ('HN1869','oropharynx',46,'male',0,'positive',2,2,0,'iva',9.3,'no','concomitant',46,2,70,35,0,2869,0,3133,0,3133,0,3133,0,3133),
    ('HN1879','oropharynx',50,'male',1,'positive',3,2,0,'iva',8.9,'no','concomitant',47,2,70,35,0,2824,0,1821,0,1821,0,1821,0,1821),
    ('HN1892','larynx',53,'female',1,NULL,1,2,0,'iva',NULL,'no','none',37,2,68,34,0,2999,0,1070,0,1070,0,1070,0,1070),
    ('HN1896','larynx',60,'male',1,NULL,3,0,0,'iii',9.4,'yes','none',38,2,68,34,1,2211,1,312,1,312,1,312,0,2211),
    ('HN1900','oropharynx',72,'male',0,'negative',4,2,0,'ivb',NULL,'no','none',44,2,70,35,1,2403,1,175,0,1332,1,175,0,1332),
    ('HN1901','oropharynx',52,'male',0,'positive',2,1,0,'iii',NULL,'no','none',37,2,68,34,0,3933,0,2004,0,2004,0,2004,0,2004),
    ('HN1910','oropharynx',76,'male',0,'negative',2,0,0,'ii',8.9,'no','none',37,2,68,34,1,4055,1,296,1,296,1,296,0,2784),
    ('HN1913','oropharynx',67,'male',NULL,'negative',2,1,0,'iii',8.8,'no','none',37,2,68,34,1,609,1,448,0,567,1,448,0,567),
    ('HN1922','larynx',57,'male',1,NULL,4,0,0,'iva',9.4,'no','none',36,2,68,34,0,3034,0,1105,0,1105,0,1105,0,1105),
    ('HN1933','oropharynx',62,'female',1,'negative',1,2,0,'iva',8.2,'no','none',37,2,68,34,1,481,0,412,0,412,0,412,0,412),
    ('HN1950','oropharynx',66,'male',1,NULL,2,1,0,'iii',NULL,'no','none',41,2,60,30,1,330,1,278,1,278,1,278,0,280),
    ('HN1954','oropharynx',77,'female',1,'negative',2,0,0,'ii',7.3,'no','none',50,2,70,35,1,4373,0,2541,0,2541,0,2541,0,2541),
    ('HN1968','larynx',47,'male',0,NULL,4,0,0,'iva',NULL,'no','none',37,2,68,34,0,3266,1,1196,0,1337,1,1196,0,1337),
    ('HN1987','oropharynx',66,'male',0,'negative',4,3,0,'ivb',9.1,'no','none',37,2,68,34,1,716,0,716,0,716,0,716,0,716),
    ('HN1998','larynx',71,'male',0,NULL,3,0,0,'iii',NULL,'no','none',37,2,68,34,0,3226,0,3081,0,3081,0,3081,0,3081);
