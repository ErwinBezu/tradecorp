# C1. Développer un dispositif de veille

Dans le cadre de ce brief, plusieurs veilles ont été réalisées en groupe, notamment sur Docker, Spark et sur Airflow. 

Pour chaque sujet de veille, nous avons commencé par une recherche individuel des informations provenant de différentes sources, principalement des documentations officielles et des ressources techniques spécialisées obtenu grace au moteur de recherche. 

Les informations recueillies ont ensuite été confrontées et croisées afin de vérifier leur cohérence et leur fiabilité. Cette étape nous a permis de sélectionner les informations les plus pertinentes.

À partir de cette sélection, nous avons réalisé un travail de synthèse et de vulgarisation afin de rendre les notions techniques compréhensibles. Les informations retenues ont ensuite été structurées dans un support de présentation.

La diffusion de la veille a été réalisée lors d'une présentation orale devant la promotion. Cette présentation permettait également un moment d'échange.

Enfin, les supports de présentation sont conservés afin de garder une trace des recherches effectuées et de pouvoir réutiliser ultérieurement les informations collectées.

Le processus de veille suivi peut ainsi être résumé de la manière suivante :

Recherche et collecte => croisement et analyse des sources => synthèse et vulgarisation => création du support => présentation à la promotion => conservation du support.


**Évaluation** : 
- Le process de veille détaille clairement les étapes de collecte, d’exploitation, diffusion et conservation de l’information.
- Les sources d’information (stratégique, concurrentielle, règlementaire, sectorielle, …) permettent de recueillir des informations alimentant le diagnostic stratégique de l’entreprise.

# C3. Identifier les attentes et besoins utilisateurs et contraintes du client final et de la DSI2

À partir du postulat fourni dans le brief, les besoins du projet ont été identifiés autour de la mise en place d’un traitement de données avec Apache Spark. La solution doit permettre de charger, contrôler, nettoyer, transformer et enrichir plusieurs sources de données avant leur chargement dans PostgreSQL. Les principales contraintes concernent l’utilisation de PySpark, l’exécution dans un environnement Dockerisé, la qualité et la cohérence des données ainsi que la traçabilité des traitements.

Le projet ne comportant pas d'interface utilisateur destinée au grand public, les problématiques d'accessibilité concernent principalement la documentation, les notebooks et les supports de restitution. Ceux-ci doivent rester lisibles et structurés.

**Évaluation** :
- La demande est clarifiée au travers de formulations justes faisant ressortir les attentes et contraintes du client. - Les processus métier et exigences clients sont formalisées. - Les situations de handicap dans lesquelles les utilisateurs pourraient se retrouver sont recherchées et considérées

# C5. Diagnostiquer la problématique, via une étude contextualisée de l’environnement interne et externe du client

L’analyse du contexte met en évidence la nécessité pour TradeCorp de disposer de données fiables et exploitables provenant de plusieurs sources. L’utilisation de fichiers séparés implique des problématiques de qualité, de cohérence et de traitement des données. La mise en place d’un pipeline avec Apache Spark permet d’automatiser les opérations de nettoyage, de transformation et d’enrichissement tout en proposant une architecture capable d’évoluer avec l’augmentation du volume de données grâce au traitement distribué. La conteneurisation avec Docker facilite également le déploiement et la reproductibilité de l’environnement. Les principales contraintes identifiées concernent les ressources nécessaires à Spark, la sécurisation des accès et des données, ainsi que la complexité supplémentaire apportée par une architecture distribuée par rapport à un traitement classique.


**Évaluation** : 
- Le diagnostic met en relation des éléments liés à son environnement interne et externe et permet de délimiter nettement une problématique ainsi qu’un niveau de solution à apporter. - Les contraintes opérationnelles, telles que la scalabilité, la performance et la sécurité, sont prises en compte dans l'analyse.
- Les opportunités dégagées répondent spécifiquement au contexte du client. Elles sont à la fois ambitieuses et atteignables (cela peut inclure l'adoption de nouvelles technologies, l'optimisation des processus, ou la réorganisation de l'architecture pour mieux répondre aux besoins stratégiques).
- Les obstacles et contraintes sont estimés dans leurs dimensions techniques et organisationnelles.
____________________________________________________________________________

# C6. Définir une stratégie data et/ou IA et un plan d’action adaptés aux enjeux du client






**Évaluation** :
- La stratégie de sécurisation de l'architecture des données est alignée sur la stratégie générale de l'entreprise ainsi que sur les besoins spécifiques des utilisateurs d'assurer que les structures de données et les modèles de données soutiennent efficacement les objectifs business et opérationnels. Une stratégie spécifique pour l'intégration de l'intelligence artificielle dans l'architecture des systèmes d'information est élaborée. Elle vise à maximiser l'exploitation des données pour générer des insights précis
.- Les interventions sur l'architecture des données sont hiérarchisées, en utilisant une approche fondée sur une évaluation rigoureuse des risques associés aux différentes composantes des systèmes d'information. - Le plan d’actions est conçu pour être robuste et précis, tout en étant suffisamment flexible pour s'adapter aux changements dans l'environnement technologique et opérationnel de l'organisation. Ce plan doit intégrer des mesures préventives et correctives pour renforcer la sécurité des données. - Les objectifs de sécurité des données - intégrité, disponibilité et confidentialité - sont définis en tenant compte du cadre réglementaire applicable et des besoins précis du client pour que l'architecture de données soit conforme et sécurisée. - Les solutions de mitigation sont mises en oeuvre en collaboration avec les équipes IT et de sécurité, en respectant les délais et les budgets prévus. - Les recommandations fournies permettent aux équipes opérationnelles de se positionner par rapport aux niveaux de service attendus en termes de disponibilité, intégrité et confidentialité des systèmes d'information, avec une attention particulière sur l'architecture des données et son évolution.
____________________________________________________________________________
# C7. Déployer une approche Green IT et Low-tech,





**Évaluation** :
- L’utilisation des ressources est minimisée et inclut la réduction de l'empreinte carbone des infrastructures IT, l'optimisation de l'efficacité énergétique des systèmes, et la mise en place de solutions low-tech.
- Les valeurs clés de la RSE sont intégrées en prenant notamment appui sur les objectifs de développement durable de l’ONU.
- Des normes sur l’écoconception des services numériques sont introduites dans le projet de sécurité informatique.
____________________________________________________________________________

# C8. Présenter la solution informatique proposée





**Évaluation** : 
- Le vocabulaire est choisi en fonction de l’environnement stratégique et opérationnel du client.
- Les gains en termes de performance et de sécurité sont clairement démontrés après l'optimisation de l’architecture.
- Le support pour la présentation orale est à la fois lisible, complet, synthétique et adapté à la cible ainsi qu’aux enjeux de la situation de présentation de la solution.
____________________________________________________________________________

# C10. Rédiger les spécifications techniques, à partir des besoins identifiés








**Évaluation** : 
L'expression des besoins data et IA est cohérente :
• Par rapport au type de structure de l'entreprise,
• En conformité avec les exigences légales et réglementaires,
• En tenant compte de la nature et du contexte spécifique du projet, notamment en ce qui concerne les données à caractère personnel (DCP),
• En alignement avec le contexte métier de l’entreprise et son évolution,
• En prenant en considération les menaces techniques, humaines et organisationnelles,
• En intégrant les nouveaux événements majeurs dans les domaines juridique, économique, politique, commercial, technologique, ou environnemental. 
- Les spécifications techniques pour la gestion des données incluent les éléments suivants : choix des technologies de données ; stratégies de stockage et options d'hébergement des données ; configuration de l'environnement et architecture des systèmes de données ; exigences de programmation spécifiques pour la manipulation des données ; normes d'accessibilité et de récupération des données ; protocoles de sécurité pour la protection des données ; maintenance des systèmes de données et stratégies pour les évolutions futures ; glossaire des termes techniques liés à la gestion des données.

# C11. Assurer la conception méthodologique d’un projet en ingénierie de données massives








**Évaluation** :
- Le choix de la méthode de gestion informatique (type agile, en V, en cascade, hybride) est argumenté et cohérent au regard de l’analyse des besoins et moyens associés. - La démarche de pilotage de projet repose sur l’utilisation de bonnes pratiques numériques notamment issues du ‘Guide des bonnes pratiques numérique responsable pour les organisations’ élaboré par la Mission interministérielle numérique écoresponsable3. - Les impacts environnementaux et sociaux sont pris en compte dans la sélection des méthodes relatives au projet informatique. - Une politique d’archivage, d’expiration et de suppression des données est définie conformément aux directives de la CNIL. - Le cadre de référence légal et règlementaire associé au projet est exhaustif.
____________________________________________________________________________

# C13. Décrire la solution informatique et ses fonctionnalités (user stories)









**Évaluation** :
- Une documentation est fournie pour chaque étape du projet : Cahier des Charges Fonctionnel, Document d’Architecture Technique, Documentation du code, Guide d’installation, Manuel utilisateur…

# C14. Contrôler et mesurer l’avancement du projet IA








**Évaluation** : 
- Les indicateurs choisis comprennent à minima des KPI de coût, délai et de ressources ainsi que des critères ESG5 - Les indicateurs choisis répondent aux exigences des approches causale ainsi qu’ effectuale. - La garantie de la qualité est permise par la vérification de la conformité aux exigences convenues :
• Celle de l’analyse pour la conformité aux spécifications de la demande,
• Celle de la conception pour la conformité aux besoins du client,
• Celle du produit final pour la conformité au cahier des charges établi en amont.
____________________________________________________________________________

# C15. Estimer et suivre les coûts tout au long du cycle de vie du projet de développement informatique







**Évaluation** : 
- Le budget est aligné sur les objectifs du projet tout en anticipant les éventuelles adaptations. - Le budget du projet est correctement analysé et fait éventuellement l’objet de préconisations par le candidat. - Les coûts de déploiement et d’exploitation de chaque étape du cycle de vie de la solution ou de l’équipement sont correctement dimensionnés.
- Les actions liées à l’environnement sont budgétées.

# C21. Evaluer tous les risques potentiels associés à la manipulation de grandes quantités de données






**Évaluation** : 
- L’évaluation des risques porte sur la sécurité, la sauvegarde, la récupération ainsi que le traitement des données.
- L'identification des risques prend en compte les aspects réglementaires, comme le respect des normes de confidentialité et de protection des données (RGPD, etc.).
____________________________________________________________________________

# C25. Concevoir et administrer des data lakes et data warehouses











**Évaluation** : 
- Les infrastructures basées sur Hadoop ou MongoDB sont conçues pour être facilement évolutives, permettant d'ajouter de nouvelles sources de données et d'augmenter les capacités de traitement sans compromettre les performances ou la stabilité. 
- L’utilisation d’Hadoop et MongoDB facilitent l'intégration des données provenant de diverses sources et leur utilisation pour des analyses avancées. 
- RDF et OWL sont utilisés de manière appropriée pour structurer les données, en créant des modèles sémantiques clairs et interopérables qui facilitent l'intégration et l'analyse des données. - Les systèmes d'ontologies permettent l'extraction de connaissances pertinentes à partir de données non structurées et semi-structurées. - Les standards de l'industrie pour la modélisation sémantique garantissent une interopérabilité maximale avec d'autres systèmes et applications basées sur la sémantique.
____________________________________________________________________________
# C26. Gérer la scalabilité et la performance des architectures de stockage,







**Évaluation** : 
- Les requêtes sont exécutées plus rapidement sur de grands volumes de données, minimisent les erreurs et assurent l'intégrité des données. - Les requêtes optimisées maintiennent des performances élevées à mesure que le volume de données augmente. - Les données sont traitées de manière plus fiable après optimisation.
____________________________________________________________________________
# C28. Mettre en œuvre des pipelines de données sécurisés






**Évaluation** : 
- Les pipelines de données garantissent une transmission cohérente et fiable des données critiques, en limitant les pertes de données et en maintenant la continuité des flux même en cas d'incidents. 
- Les données sont traitées et disponibles en temps réel. - Les pipelines de données sont sécurisés grâce à des techniques appropriées, telles que : le chiffrement, les contrôles d'accès, etc.
____________________________________________________________________________
# C30. Déployer et optimiser des infrastructures sur Amazon AWS, Google Cloud ou Azure









**Évaluation** : 
- Les solutions cloud déployées sont capables de s'adapter dynamiquement aux variations de la demande, optimisent l'allocation des ressources et garantissent une disponibilité continue et une performance élevée, même lors des pics de charge. 
- Les clusters de calcul sont configurés pour maximiser la puissance de traitement et permettent des calculs intensifs sans compromettre la réactivité des applications. 
- Le stockage distribué est géré de manière efficace : il assure une accessibilité rapide aux données tout en minimisant les coûts liés à la consommation de ressources. 
- Les systèmes mis en place démontrent une capacité à évoluer facilement, supportant une augmentation du volume de données et des utilisateurs sans dégradation des performances.
____________________________________________________________________________
# C31. Concevoir et optimiser des pipelines de données dans le cloud







**Évaluation** : 
- Les pipelines de données sont conçus pour automatiser efficacement l'ingestion, la transformation, et la distribution des données, garantissent une exécution fluide et sans interruption des processus de bout en bout. 
- La rapidité et la fiabilité des flux de données sont maintenues même sous des charges de travail élevées.
- Les outils tels que AWS Data Pipeline et Azure Data Factory sont utilisés de manière optimale, permettant une gestion flexible et évolutive des pipelines pour répondre aux besoins croissants de l'entreprise.
____________________________________________________________________________

# C34. Assurer le nettoyage, la transformation et la réduction de la dimensionnalité des datasets







**Évaluation** : 
- Le nettoyage et la transformation des données sont effectués avec précision : éliminent les anomalies, les valeurs manquantes et les incohérences, ce qui améliore la qualité des données et la fiabilité des analyses. 
- Les techniques de réduction de la dimensionnalité sont appliquées efficacement, optimisant les performances des modèles d'IA en réduisant la complexité des datasets sans perdre d'information critique.
____________________________________________________________________________
# C39. Effectuer l’analyse exploratoire des données






**Évaluation** : 
- L’analyse exploratoire des données est réalisée avec précision à l’aide d’outils tels que Jupyter Notebook et RStudio, Les résultats de l’analyse sont interprétés de manière pertinente, offrant une compréhension approfondie des données et facilitant l’élaboration de modèles prédictifs robustes.