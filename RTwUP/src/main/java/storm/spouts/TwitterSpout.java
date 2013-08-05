package storm.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.URLEntity;
import twitter4j.auth.AccessToken;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import utils.BoundingBox;

/** 
 * 
 * This spout listens to tweet stream, then filters the tweets by location (e.g. city of Rome)
 * and retrieves only the links contained in tweets.
 * 
 * @author Gabriele de Capoa, Gabriele Proni, Daniele Morgantini
 * 
 * **/

public class TwitterSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterSpout.class);

    private LinkedBlockingQueue<Status> queue = null;
	private SpoutOutputCollector collector;
	private TwitterStream ts = null;
    private BoundingBox observedRegion;

    @Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {

		double[][] coordinates = new double[2][2];
		coordinates[0][0] = (Double) conf.get("sw0");
		coordinates[0][1] = (Double) conf.get("sw1");
		coordinates[1][0] = (Double) conf.get("ne0");
		coordinates[1][1] = (Double) conf.get("ne1");

        this.observedRegion = new BoundingBox(coordinates);

		this.queue = new LinkedBlockingQueue<Status>();
		this.collector = collector;
		this.ts = new TwitterStreamFactory().getInstance();
		this.ts.setOAuthConsumer("P9c5PqNZ2HvANU6B8Rrp1A", "iKUCqCYbvI8Tam7zGgIRiO6Zcyh2hw7Nm0v97lE");
		AccessToken accessToken = new AccessToken("1546231212-TKDS2JM9sBp351uEuvnbn1VSPLR5mUKhZxwmfLr","8krjiVUEAoLvFrLC8ryw8iaU2PKTU80WHZaWevKGk2Y");
		this.ts.setOAuthAccessToken(accessToken);

		final StatusListener listener = new StatusListener() {

            @Override
			public void onException(Exception e) {
                LOGGER.warn("listener exception: " + e.getMessage());
			}

            @Override
			public void onDeletionNotice(StatusDeletionNotice notice) {
			}

            @Override
			public void onScrubGeo(long arg0, long arg1) {
			}

            @Override
			public void onStallWarning(StallWarning warning) {
                LOGGER.warn("stall warning: " + warning.getMessage());
			}

            @Override
			public void onStatus(Status status) {

				if(status.getURLEntities().length != 0) {

            		final GeoLocation statusGl = status.getGeoLocation();

                    if(statusGl != null && observedRegion.includes(statusGl)) {
                        queue.add(status);
                    }
				}
			}

            @Override
			public void onTrackLimitationNotice(int skippedStatuses) {
                LOGGER.warn("track limit. skipped '" + skippedStatuses + "' statuses");
			}

		};

		this.ts.addListener(listener);
		FilterQuery query = new FilterQuery();
		query.locations(observedRegion.getCoordinates());
		this.ts.filter(query);
	}

    @Override
	public void nextTuple() {
		try {
			Status retrieve = queue.take();
			URLEntity[] urls = retrieve.getURLEntities();
			for (URLEntity url : urls) {
				this.collector.emit(new Values(url.getExpandedURL()));
            }
		} catch (InterruptedException e) {
			LOGGER.error("ERRORE SULLO SPOUT: " + e.getMessage());
		}

	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
	}

    @Override
	public void close(){
		this.ts.shutdown();
	} 
}
