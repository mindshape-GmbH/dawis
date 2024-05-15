# DAWIS – das Open-Source Data Warehouse System

## Requirements

* Python 3.9
* Pipenv 2022.1
* MongoDB 4.2
* MySQL 5.7
* [ChromeDriver](https://sites.google.com/a/chromium.org/chromedriver/home) (stable release)

Überall hört man derzeit von „data-driven SEO“. Bei mindshape arbeiten wir seit Jahren datengetrieben und haben dazu schon eine Menge Vorträge gehalten und Publikationen veröffentlicht.

Dabei haben wir immer wieder feststellen müssen, dass in Unternehmen und Agenturen häufig das technische Handwerkszeug fehlt, um die notwendigen Daten zu erfassen, zu speichern und auszuwerten. Und nicht jeder programmiert sich das eben mal so. Bei mindshape haben wir das bis vor Kurzem noch mit einer gewachsenen Struktur aus einzelnen Programmen, PHP- und python-Skripts gelöst. Das war uns allerdings nicht mehr gut genug und wir wollten außerdem einen Beitrag schaffen, damit andere SEOs in die Lage versetzt werden, wirklich „data-driven SEO“ zu betreiben.

## Was ist DAWIS?

Das war die Geburtsstunde von DAWIS – dem Data Warehouse and Information System. Das System ist kostenlos und quelloffen, d.h. jeder kann und soll es für sich nutzen, adaptieren und ist herzlich eingeladen, es mit weiter zu entwickeln oder Feedback zu geben. Es ist vollständig in python geschrieben und kann auf jedem Rechner (Linux, Windows, Mac) eingerichtet werden. Es handelt sich allerdings nicht um ein „installierbares Programm“ wie z.B. der ScreamingFrog, sondern es ist als Serveranwendung konzeptioniert, um große Mengen an Daten verarbeiten zu können.

DAWIS liest eine Konfigurationsdatei mit einem bestimmten Format ein (YAML). Dort kannst Du die verschiedenen Module, Datenbanken, Monitorings und alle anderen Einstellungen tätigen. Wenn Du die vorhandenen DAWIS-Module und -Funktionen nutzen möchtest, brauchst Du auch nicht mehr zu tun. Easy. Das ist also der erste Schritt – Deine eigene config.yaml anlegen! Damit das einfach geht, haben wir Dir eine Beispiel-Vorlage in der technischen Dokumentation mitgeliefert.

Wenn Dir der aktuelle Funktionsumfang von DAWIS nicht ausreicht, hast Du verschiedene Möglichkeiten. Entweder Du programmierst neue Funktionen selbst hinzu (und beteiligst Dich vielleicht damit an der Weiterentwicklung von DAWIS). Oder Du schreibst uns Deine Idee und wir nehmen sie mit auf in den Ideen-Wünsche-Pool.

## Funktionsweise und verfügbare Module?

DAWIS besteht aus verschiedenen Modulen. Aktuell haben wir die ersten Module als MVP mit veröffentlicht, d.h. sie haben noch nicht den vollen Funktionsumfang. Es gibt z.B. ein HTML-Modul, um HTML-Dateien zu parsen oder ein robots.txt-Modul. Hier erscheinen regelmäßig Updates, aber wir wollten das Framework so früh wie möglich nach der Betaphase online stellen, damit wir nach dem Lean-Prinzip das Feedback mitberücksichtigen können. Insofern ist das aktuelle DAWIS-Framework als MVP zu verstehen.

Derzeit gibt es folgende funktionale Module:

* HTML-Parser (crawler): Hiermit kannst Du bestimmte HTML-Tags prüfen, etwa Canonical-Tags oder Noindex-Anweisungen.
* Robots.txt Monitoring (robotstxt): Hier kannst Du z.B. überprüfen, ob die robots.txt vorhanden ist oder ob eine XML-Sitemap vorhanden und erreichbar ist. Später wirst Du hier auch sehen können, ob es eine Änderung an der robots.txt gab (und welche).
* Pagespeed API (pagespeed): Hier kannst Du für bestimmte URLs regelmäßig die Google Pagespeed-API abfragen, um die Geschwindigkeit Deiner zentralen URLs zu monitoren.

In Entwicklung bzw. Finalisierung sind noch weitere Funktionen in den jeweiligen Modulen bzw. auch noch ganz neue Module, wie etwa ein GSC-Modul, welches die GSC-Search-Daten in BigQuery speichert. Auf weitere Anregungen und Ideen freuen wir uns jederzeit.

### Programmlogik:

Wir unterscheiden bei der Programmierlogik zwischen zwei Ebenen:

* „Aggregations“ ist die Datenerfassung, welche je nach Modul die erfassten Daten (z.B. eine HTML-Datei) in den Zwischencache (MongoDB) zur weiteren Verarbeitung speichert.
* „Operations“ ist gewissermaßen die Auswertungsebene, bei der Daten ausgewertet und final gespeichert werden in eine relationale Datenstruktur (z.B. mySQL) oder in BigQuery.

### URL-Sets

Die Grundlage für die Verarbeitung von Daten sind URLs, auf die zugegriffen wird. DAWIS beherrscht sogenannte URL-Sets, d.h. eine Menge an URLs, die gemeinsam behandelt werden kann. So kannst Du zum Beispiel ein URL-Set für wichtige Kategorie-Seiten in Deinem Shop anlegen, oder ein URL-Set für verschiedene Produktdetailseiten. Auf diesen kann man dann gemeinsame Operationen wie z.B. die Prüfung, ob der Canonical-Tag noch vorhanden ist, durchführen. DAWIS kann nicht selbstständig eine Website crawlen und „alle URLs“ behandeln. Das ist auch nicht Sinn und Zweck eines Monitoring- und Alerting-Systems.

## Docker Setup

Um Dawis mit Docker zu starten, sind folgende Schritte notwending:

* `docker build -t dawis .`
* `docker compose up -d`

Unter `http://localhost:4321` können Daten in MongoDB eingesehen werden, unter `localhost:8090` Daten in MySQL.

Die Konfiguration befindet sich im Ordner `config`. Logs sind im Ordner `log` zu finden.

Beim Starten wird die Datei `delete_me_for_restart` angelegt. Wenn beispielsweise eine Dateifreigabe auf den `config`-Ordner eingerichtet ist, kann Dawis so auf einfache Art und Weise neugestartet werden, nachdem Konfiguration geändert wurde. Dawis empfängt ein SIGINT-Signal und kann kontrolliert seine Prozesse abschließen und neu starten.

Die Ressourcen-Limits können je nach Bedarf in der `docker-compose.yml` erhöht oder weiter eingeschränkt werden, je nach Performance-Anforderungen und Leistung des Servers.

### Debugging

Zum Debuggen kann folgender Befehl ausgeführt werden: `docker compose up --profile debug dawis_debug`

## Nächste Schritte

Was kommt als Nächstes? Wir sammeln nach dem Lean-Prinzip das Feedback weiter ein. Parallel erweitern wir den Funktionsumfang der vorhandenen Module. Im Fokus stehen diese Bereiche:

* Funktionsumfang vorhandene Module erweitern

* Alerting erweitern um automatische Mails bei Alert-Fall

* GSC-Modul soweit fertig machen für die Erst-Veröffentlichung

Außerdem veröffentlichen wir zeitnah ein paar Beispiel-Dashboards (Google Data Studio) als Demo, was man mit den gespeicherten Daten alles visualisieren kann.

## Feedback

Hast Du Anregungen und Ideen? Möchtest Du mitentwickeln? Oder hast Du einen Wunsch zur Funktionserweiterung? Dann schreib uns gern an dawis@mindshape.de
