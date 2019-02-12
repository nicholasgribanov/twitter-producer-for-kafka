package name.nicholasgribanov.twitter.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private final static Logger log = LoggerFactory.getLogger(TwitterProducer.class);

    private TwitterProducer() {
    }

    public static void main(String[] args) throws IOException {
        new TwitterProducer().run();
    }

    private void run() throws IOException {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Properties twitterProperties = new Properties();
        twitterProperties.load(new FileInputStream(new File("src/main/resources/twitter.properties").getAbsolutePath()));

        Client client = createTwitterClient(twitterProperties, msgQueue);
        client.connect();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                log.info("msg");
            }

        }
        log.info("Application stopped");
    }

    private Client createTwitterClient(Properties properties, BlockingQueue<String> msgQueue) {


        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(properties.getProperty("consumerKey"),
                properties.getProperty("consumerSecret"),
                properties.getProperty("token"),
                properties.getProperty("secret"));

        //TODO setting real authentication data
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

}
