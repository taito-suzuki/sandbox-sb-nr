newrelic-java.zip:
	curl -O https://download.newrelic.com/newrelic/java-agent/newrelic-agent/current/newrelic-java.zip
newrelic/: newrelic-java.zip
	unzip $<

clean:
	rm -rf newrelic-java newrelic-java.zip