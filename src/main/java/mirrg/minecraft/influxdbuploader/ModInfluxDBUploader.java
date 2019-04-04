package mirrg.minecraft.influxdbuploader;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import mirrg.boron.util.UtilsMath;
import mirrg.boron.util.suppliterator.ISuppliterator;
import net.minecraft.entity.EntityLivingBase;
import net.minecraft.entity.EnumCreatureType;
import net.minecraft.entity.item.EntityItem;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.entity.player.EntityPlayerMP;
import net.minecraft.item.ItemStack;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.World;
import net.minecraft.world.chunk.Chunk;
import net.minecraft.world.gen.ChunkProviderServer;
import net.minecraftforge.common.ForgeChunkManager.Ticket;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.common.config.Configuration;
import net.minecraftforge.event.CommandEvent;
import net.minecraftforge.event.ServerChatEvent;
import net.minecraftforge.event.entity.item.ItemExpireEvent;
import net.minecraftforge.event.entity.living.LivingDeathEvent;
import net.minecraftforge.event.entity.living.LivingDestroyBlockEvent;
import net.minecraftforge.event.world.ChunkEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.Mod.EventHandler;
import net.minecraftforge.fml.common.event.FMLInitializationEvent;
import net.minecraftforge.fml.common.event.FMLPreInitializationEvent;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;
import net.minecraftforge.fml.common.gameevent.TickEvent.WorldTickEvent;

@Mod(modid = ModInfluxDBUploader.MODID, name = ModInfluxDBUploader.NAME, version = ModInfluxDBUploader.VERSION, acceptableRemoteVersions = "*")
public class ModInfluxDBUploader
{

	public static final String MODID = "mirrg.minecraft.influxdbuploader";
	public static final String NAME = "InfluxDBUploader";
	public static final String VERSION = "0.0.6";

	private static Logger logger;

	private static boolean enableUploading;
	private static String url;
	private static String userName;
	private static String password;
	private static String database;
	private static String serverName;

	@EventHandler
	public void preInit(FMLPreInitializationEvent event)
	{
		logger = event.getModLog();

		{
			Configuration configuration = new Configuration(event.getSuggestedConfigurationFile());
			enableUploading = configuration.getBoolean("enableUploading", "general", false, "If true, uploading is enable");
			url = configuration.getString("url", "connection", "http://example.com/", "InfluxDB Uploading URL");
			userName = configuration.getString("userName", "connection", "userName", "User name for connection");
			password = configuration.getString("password", "connection", "password", "Password for the user");
			database = configuration.getString("database", "connection", "database001", "Database name of InfluxDB");
			serverName = configuration.getString("serverName", "data", "Minecraft Server", "Minecraft server name");
			configuration.save();
		}

		influxDb = InfluxDBFactory.connect(url, userName, password);
		influxDb.setDatabase(database);
		startSender();

		MinecraftForge.EVENT_BUS.register(new Object() {
			@SubscribeEvent
			public void handle(ItemExpireEvent event)
			{
				if (!enableUploading) return;

				try {
					Point.Builder builder = Point.measurement("event");

					builder.tag("SERVER", serverName);
					builder.addField("server", serverName);
					builder.tag("TYPE", "itemExpire");
					builder.addField("type", "itemExpire");

					ItemStack itemStack = event.getEntityItem().getItem();
					builder.addField("sender", itemStack.getDisplayName());

					builder.addField("item", itemStack.getItem().getRegistryName().toString());
					builder.addField("metadata", itemStack.getMetadata());
					builder.addField("count", itemStack.getCount());
					builder.addField("hasNbt", itemStack.hasTagCompound());
					builder.addField("uuid", event.getEntityItem().getUniqueID().toString());
					builder.addField("dimension", event.getEntityItem().dimension);
					builder.addField("x", event.getEntityItem().posX);
					builder.addField("y", event.getEntityItem().posY);
					builder.addField("z", event.getEntityItem().posZ);

					builder.addField("message", String.format("%s:%s*%s%s@(%.0f,%.0f,%.0f@DIM%d)",
						itemStack.getItem().getRegistryName().toString(),
						itemStack.getMetadata(),
						itemStack.getCount(),
						itemStack.hasTagCompound() ? "(NBT)" : "",
						event.getEntityItem().posX,
						event.getEntityItem().posY,
						event.getEntityItem().posZ,
						event.getEntityItem().dimension));
					{
						NBTTagCompound nbt = new NBTTagCompound();
						itemStack.writeToNBT(nbt);
						builder.addField("message_long", nbt.toString());
					}

					sendPoint(builder.build());
				} catch (Exception e) {
					logger.error("InfluxDB Upload Error(1): " + e.getMessage());
				}
			}
		});

		MinecraftForge.EVENT_BUS.register(new Object() {
			@SubscribeEvent
			public void handle(ServerChatEvent event)
			{
				if (!enableUploading) return;

				try {
					Point.Builder builder = Point.measurement("message");

					builder.tag("SERVER", serverName);
					builder.addField("server", serverName);
					builder.tag("TYPE", "chat");
					builder.addField("type", "chat");

					builder.addField("sender", event.getUsername());
					builder.addField("message", event.getMessage());

					sendPoint(builder.build());
				} catch (Exception e) {
					logger.error("InfluxDB Upload Error(2): " + e.getMessage());
				}
			}
		});

		MinecraftForge.EVENT_BUS.register(new Object() {
			@SubscribeEvent
			public void handle(CommandEvent event)
			{
				if (!enableUploading) return;

				try {
					Point.Builder builder = Point.measurement("message");

					builder.tag("SERVER", serverName);
					builder.addField("server", serverName);
					builder.tag("TYPE", "command");
					builder.addField("type", "command");

					builder.addField("sender", event.getSender().getName());
					builder.addField("message", "/" + ISuppliterator.concat(
						ISuppliterator.of(event.getCommand().getName()),
						ISuppliterator.ofObjArray(event.getParameters()))
						.join(" "));

					sendPoint(builder.build());
				} catch (Exception e) {
					logger.error("InfluxDB Upload Error(3): " + e.getMessage());
				}
			}
		});

		MinecraftForge.EVENT_BUS.register(new Object() {
			Map<World, LocalDateTime> timeLastTable = new HashMap<>();

			@SubscribeEvent
			public void handle(WorldTickEvent event)
			{
				if (!enableUploading) return;

				LocalDateTime timeLast = timeLastTable.get(event.world);

				if (timeLast == null) {
					timeLast = LocalDateTime.now();
				}
				LocalDateTime timeNow = LocalDateTime.now();

				if (!floor5seconds(timeLast).equals(floor5seconds(timeNow))) {
					onTime(event);
				}

				if (!floor1minute(timeLast).equals(floor1minute(timeNow))) {

					// 強制ロード
					event.world.getPersistentChunks().forEach((chunkPos, ticket) -> {
						sendChunkLoader(event.world, chunkPos, ticket);
					});

					// ロード中
					if (event.world.getChunkProvider() instanceof ChunkProviderServer) {
						for (Chunk chunk : ((ChunkProviderServer) event.world.getChunkProvider()).getLoadedChunks()) {
							sendChunkLoaded(event.world, chunk);
						}
					}

				}

				timeLastTable.put(event.world, timeNow);
			}

			private void sendChunkLoader(World world, ChunkPos chunkPos, Ticket ticket)
			{
				try {

					Point.Builder builder = Point.measurement("forcedchunk");

					builder.tag("SERVER", serverName);
					builder.addField("server", serverName);
					builder.tag("MOD", ticket.getModId());
					builder.addField("mod", ticket.getModId());
					builder.tag("PLAYER", "" + ticket.getPlayerName());
					builder.addField("player", "" + ticket.getPlayerName());
					builder.tag("WORLD_ID", "" + world.provider.getDimension());
					builder.addField("world_id", world.provider.getDimension());

					builder.addField("chunk_x", chunkPos.x);
					builder.addField("chunk_z", chunkPos.z);

					sendPoint(builder.build());

				} catch (Exception e) {
					logger.error("InfluxDB Upload Error(4): " + e.getMessage());
				}
			}

			private void sendChunkLoaded(World world, Chunk chunk)
			{
				try {

					Point.Builder builder = Point.measurement("loadedchunk");

					builder.tag("SERVER", serverName);
					builder.addField("server", serverName);
					builder.tag("WORLD_ID", "" + world.provider.getDimension());
					builder.addField("world_id", world.provider.getDimension());

					builder.addField("chunk_x", chunk.x);
					builder.addField("chunk_z", chunk.z);

					sendPoint(builder.build());

				} catch (Exception e) {
					logger.error("InfluxDB Upload Error(4): " + e.getMessage());
				}
			}

			private LocalDateTime floor5seconds(LocalDateTime time)
			{
				time = time.withSecond(time.getSecond() / 5 * 5);
				time = time.withNano(0);
				return time;
			}

			private LocalDateTime floor1minute(LocalDateTime time)
			{
				time = time.withSecond(0);
				time = time.withNano(0);
				return time;
			}

			private void onTime(WorldTickEvent event)
			{
				if (event.world.isRemote) return;

				try {

					// world
					send(event);

					// players
					for (EntityPlayer player : event.world.playerEntities) {
						if (player instanceof EntityPlayerMP) {
							EntityPlayerMP playerMP = (EntityPlayerMP) player;

							Point.Builder builder = Point.measurement("player");

							builder.tag("SERVER", serverName);
							builder.addField("server", serverName);
							builder.tag("PLAYER_UUID", playerMP.getUniqueID().toString());
							builder.addField("player_uuid", playerMP.getUniqueID().toString());
							builder.tag("PLAYER_NAME", playerMP.getName());
							builder.addField("player_name", playerMP.getName());
							builder.tag("PLAYER_DISPLAY_NAME", playerMP.getDisplayNameString());
							builder.addField("player_displayName", playerMP.getDisplayNameString());

							builder.addField("player_dimension", playerMP.dimension);
							builder.addField("player_x", playerMP.posX);
							builder.addField("player_y", playerMP.posY);
							builder.addField("player_z", playerMP.posZ);

							builder.addField("player_invisible", playerMP.isInvisible() ? 1 : 0);
							builder.addField("player_health", playerMP.getHealth());
							builder.addField("player_maxHealth", playerMP.getMaxHealth());
							builder.addField("player_experienceLevel", playerMP.experienceLevel);
							builder.addField("player_gamemode", playerMP.interactionManager.getGameType().getName());
							builder.addField("player_allowEdit", playerMP.capabilities.allowEdit);
							builder.addField("player_allowFlying", playerMP.capabilities.allowFlying);
							builder.addField("player_disableDamage", playerMP.capabilities.disableDamage);
							builder.addField("player_isCreativeMode", playerMP.capabilities.isCreativeMode);
							builder.addField("player_isFlying", playerMP.capabilities.isFlying);

							sendPoint(builder.build());
						}
					}

				} catch (Exception e) {
					logger.error("InfluxDB Upload Error(4): " + e.getMessage());
				}
			}

			private void send(WorldTickEvent event)
			{
				Point.Builder builder = Point.measurement("world");

				builder.tag("SERVER", serverName);
				builder.addField("server", serverName);

				builder.tag("WORLD_ID", "" + event.world.provider.getDimension());
				builder.addField("world_id", event.world.provider.getDimension());
				builder.tag("WORLD_NAME", event.world.provider.getDimensionType().getName());
				builder.addField("world_name", event.world.provider.getDimensionType().getName());

				builder.addField("time_tick", event.world.getWorldTime());
				builder.addField("time_day", event.world.getWorldTime() / 24000);
				builder.addField("time_tickOfDay", event.world.getWorldTime() % 24000);
				builder.addField("time_clock", String.format("%02d:%02d",
					(event.world.getWorldTime() % 24000) / 1000,
					UtilsMath.trim((int) (((event.world.getWorldTime() % 24000) % 1000) / 1000.0 * 60.0), 0, 59)));
				builder.addField("time_raining", event.world.isRaining());
				builder.addField("time_daytime", event.world.isDaytime());
				builder.addField("time_thundering", event.world.isThundering());
				builder.addField("time_moonPhase", event.world.provider.getMoonPhase(event.world.getWorldTime()));

				builder.addField("count_chunks", event.world.getChunkProvider() instanceof ChunkProviderServer
					? ((ChunkProviderServer) event.world.getChunkProvider()).getLoadedChunkCount()
					: -1);

				builder.addField("count_tileEntities", event.world.loadedTileEntityList.size());

				builder.addField("count_entities", event.world.loadedEntityList.size());
				builder.addField("count_entities_monster", event.world.countEntities(EnumCreatureType.MONSTER, false));
				builder.addField("count_entities_creature", event.world.countEntities(EnumCreatureType.CREATURE, false));
				builder.addField("count_entities_waterCreature", event.world.countEntities(EnumCreatureType.WATER_CREATURE, false));
				builder.addField("count_entities_ambient", event.world.countEntities(EnumCreatureType.AMBIENT, false));
				builder.addField("count_entities_player", event.world.playerEntities.size());
				builder.addField("count_entities_item", event.world.loadedEntityList.stream().filter(e -> e instanceof EntityItem).count());

				sendPoint(builder.build());

			}
		});

		MinecraftForge.EVENT_BUS.register(new Object() {
			@SubscribeEvent
			public void handle(LivingDeathEvent event)
			{
				if (!enableUploading) return;

				try {
					Point.Builder builder = Point.measurement("event");

					builder.tag("SERVER", serverName);
					builder.addField("server", serverName);
					builder.tag("TYPE", "livingDeath");
					builder.addField("type", "livingDeath");

					EntityLivingBase entity = event.getEntityLiving();
					builder.addField("sender", entity.getName());

					builder.addField("class", entity.getClass().getName());
					builder.addField("uuid", entity.getUniqueID().toString());
					builder.addField("dimension", entity.dimension);
					builder.addField("x", entity.posX);
					builder.addField("y", entity.posY);
					builder.addField("z", entity.posZ);

					builder.addField("message", String.format("%s: \"%s\" @(%.0f,%.0f,%.0f@DIM%d)",
						entity.getName(),
						event.getSource() != null
							? event.getSource().getDeathMessage(entity).getUnformattedText()
							: "unspecified death message",
						entity.posX,
						entity.posY,
						entity.posZ,
						entity.dimension));
					{
						NBTTagCompound nbt = new NBTTagCompound();
						entity.writeToNBT(nbt);
						builder.addField("message_long", nbt.toString());
					}

					sendPoint(builder.build());
				} catch (Exception e) {
					logger.error("InfluxDB Upload Error(1): " + e.getMessage());
				}
			}
		});

		MinecraftForge.EVENT_BUS.register(new Object() {
			@SubscribeEvent
			public void handle(LivingDestroyBlockEvent event)
			{
				if (!enableUploading) return;

				try {
					Point.Builder builder = Point.measurement("event");

					builder.tag("SERVER", serverName);
					builder.addField("server", serverName);
					builder.tag("TYPE", "livingDestroyBlock");
					builder.addField("type", "livingDestroyBlock");

					EntityLivingBase entity = event.getEntityLiving();
					builder.addField("sender", entity.getName());

					builder.addField("class", entity.getClass().getName());
					builder.addField("uuid", entity.getUniqueID().toString());
					builder.addField("state", event.getState().toString());
					builder.addField("dimension", entity.dimension);
					builder.addField("x", event.getPos().getX());
					builder.addField("y", event.getPos().getY());
					builder.addField("z", event.getPos().getZ());

					builder.addField("message", String.format("(%s).destroy!(%s):@(DIM%d,%.0f,%.0f,%.0f)",
						entity.getName(),
						event.getState().toString(),
						entity.dimension,
						entity.posX,
						entity.posY,
						entity.posZ));

					sendPoint(builder.build());
				} catch (Exception e) {
					logger.error("InfluxDB Upload Error(1): " + e.getMessage());
				}
			}
		});

		MinecraftForge.EVENT_BUS.register(new Object() {
			@SubscribeEvent
			public void handle(ChunkEvent.Load event)
			{
				if (!enableUploading) return;

				try {
					Point.Builder builder = Point.measurement("event");

					builder.tag("SERVER", serverName);
					builder.addField("server", serverName);
					builder.tag("TYPE", "chunkLoad");
					builder.addField("type", "chunkLoad");

					builder.addField("sender", String.format("%s,%s",
						event.getChunk().x,
						event.getChunk().z));

					builder.addField("dimension", event.getWorld().provider.getDimension());
					builder.addField("chunk_x", event.getChunk().x);
					builder.addField("chunk_z", event.getChunk().z);

					builder.addField("message", String.format("load! (%s,%s):",
						event.getChunk().x,
						event.getChunk().z));

					sendPoint(builder.build());
				} catch (Exception e) {
					logger.error("InfluxDB Upload Error(1): " + e.getMessage());
				}
			}

			@SubscribeEvent
			public void handle(ChunkEvent.Unload event)
			{
				if (!enableUploading) return;

				try {
					Point.Builder builder = Point.measurement("event");

					builder.tag("SERVER", serverName);
					builder.addField("server", serverName);
					builder.tag("TYPE", "chunkUnload");
					builder.addField("type", "chunkUnload");

					builder.addField("sender", String.format("%s,%s",
						event.getChunk().x,
						event.getChunk().z));

					builder.addField("dimension", event.getWorld().provider.getDimension());
					builder.addField("chunk_x", event.getChunk().x);
					builder.addField("chunk_z", event.getChunk().z);

					builder.addField("message", String.format("unload! (%s,%s):",
						event.getChunk().x,
						event.getChunk().z));

					sendPoint(builder.build());
				} catch (Exception e) {
					logger.error("InfluxDB Upload Error(1): " + e.getMessage());
				}
			}
		});

	}

	//

	private InfluxDB influxDb;
	private Deque<Point> points = new ArrayDeque<>();

	public void startSender()
	{
		Thread thread = new Thread(() -> {
			try {
				while (true) {

					// たまっているポイントを掬う
					List<Point> points2 = new ArrayList<>();
					synchronized (points) {
						points2.addAll(points);
						points.clear();
					}

					// ポイントがたまっていたら全部吐き出す（吐き出しはデーモンスレッドではないので中断されない）
					if (!points2.isEmpty()) runSending(points2);

					// ポイントがたまっていなかったら待つ
					synchronized (points) {
						if (points.isEmpty()) {
							points.wait();
						}
					}

				}
			} catch (InterruptedException e) {

			}
		}, "InfluxDB Daemon Thread");
		thread.setDaemon(true);
		thread.start();
	}

	public void runSending(List<Point> points) throws InterruptedException
	{
		Thread thread = new Thread(() -> {
			influxDb.write(BatchPoints.database(database)
				.points(ISuppliterator.ofIterable(points).toArray(Point[]::new))
				.build());
		}, "InfluxDB Sending Thread");
		thread.start();
		thread.join();
	}

	public void sendPoint(Point point)
	{
		synchronized (points) {
			points.addLast(point);
			points.notify();
		}
	}

	//

	@EventHandler
	public void init(FMLInitializationEvent event)
	{

	}

}
