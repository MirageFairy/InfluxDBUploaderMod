package mirrg.minecraft.influxdbuploader;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import mirrg.boron.util.suppliterator.ISuppliterator;

public class InfluxDBUploader
{

	private final Logger logger;
	private final ISupplier<InfluxDB> sInfluxDb;
	private final String database;

	public InfluxDBUploader(Logger logger, ISupplier<InfluxDB> sInfluxDb, String database)
	{
		this.logger = logger;
		this.sInfluxDb = sInfluxDb;
		this.database = database;
	}

	public int tryLimit = 10;
	public int msTryWait = 10_000;
	private InfluxDB influxDb;
	private Deque<Point> pointsWaiting = new ArrayDeque<>();
	private volatile boolean started = false;
	private volatile boolean fail = false;

	private final Object lock2 = new Object();

	/**
	 * 送信デーモンを開始します。
	 * 複数回呼び出した場合、最初以外は何も行わずに戻ります。
	 */
	private void startSender()
	{
		synchronized (lock2) {
			if (started) return;
			started = true;
		}

		Thread thread = new Thread(new Runnable() {
			@Override
			public void run()
			{
				try {

					// 接続
					{
						Optional<InfluxDB> oInfluxDb = connect();
						if (!oInfluxDb.isPresent()) {
							logger.error("Could not connect to the InfluxDB server!");
							synchronized (pointsWaiting) {
								fail = true;
							}
							return;
						}
						influxDb = oInfluxDb.get();
					}

					while (true) {

						// たまっているポイントを掬う
						List<Point> points2 = new ArrayList<>();
						synchronized (pointsWaiting) {
							points2.addAll(pointsWaiting);
							pointsWaiting.clear();
						}

						// ポイントがたまっていたら全部吐き出す（吐き出しはデーモンスレッドではないので中断されない）
						if (!points2.isEmpty()) runSending(points2);

						// ポイントがたまっていなかったら待つ
						synchronized (pointsWaiting) {
							if (pointsWaiting.isEmpty()) {
								pointsWaiting.wait();
							}
						}

					}
				} catch (InterruptedException e) {

				}
			}

			private Optional<InfluxDB> connect() throws InterruptedException
			{
				for (int i = 0; i < tryLimit; i++) {
					try {
						InfluxDB influxDB = sInfluxDb.get();
						if (influxDB != null) {
							return Optional.of(influxDB);
						} else {
							logger.error("InfluxDB connection error", new NullPointerException());
						}
					} catch (Exception e1) {
						logger.error("InfluxDB connection error", e1);
					}
					Thread.sleep(msTryWait);
				}
				return Optional.empty();
			}
		}, "InfluxDB Uploader Daemon Thread");
		thread.setDaemon(true);
		thread.start();

		logger.info("InfluxDB Uploader Daemon Thread started");
	}

	private void runSending(List<Point> points) throws InterruptedException
	{
		Thread thread = new Thread(() -> {
			influxDb.write(BatchPoints.database(database)
				.points(ISuppliterator.ofIterable(points).toArray(Point[]::new))
				.build());
		}, "InfluxDB Uploader Writing Thread");
		thread.start();
		thread.join();
	}

	/**
	 * ポイントを送信待ちに追加します。
	 * このメソッドはスレッドセーフです。
	 * デーモンが開始されていない場合、開始します。
	 */
	public void sendPoint(Point... points)
	{
		synchronized (pointsWaiting) {
			if (!fail) {
				for (Point point : points) {
					pointsWaiting.addLast(point);
				}
				pointsWaiting.notify();
			}
		}
		startSender();
	}

	public static interface ISupplier<T>
	{

		public T get() throws Exception;

	}

	public static long toNanos(Instant instant)
	{
		return instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
	}

}
