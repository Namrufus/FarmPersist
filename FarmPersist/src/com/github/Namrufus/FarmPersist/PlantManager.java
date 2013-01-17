package com.github.Namrufus.FarmPersist;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.bukkit.scheduler.BukkitTask;

public class PlantManager {
	FarmPersist plugin;
	
	// database connection
	Connection conn;
	
	// map of chunk coordinates to plant chunk data
	// an entry of null denotes a chunk that is in the database but is unloaded
	HashMap<Coords, PlantChunk> chunks;
	
	// plantchunks are added to these queues when their corresponding minecraft chunk is unloaded or loaded
	// then tasks periodically actually dequeue and processes the chunk
	Queue<Coords> unloadChunks;
	Queue<Coords> loadChunks;
	
	// plants chunks to be unloaded in batches
	ArrayList<Coords> batchUnload;
	
	// the minimum response time of the loading(unloading) process in ticks, as well as
	// the maximum period of the task in ticks
	long minLoadTime;
	long maxLoadTime;
	long minUnloadTime;
	long maxUnloadTime;
	
	// task to periodically unload  load to disk plant chunks in the load and unload queues
	// the rescheduler tasks periodically run to change the rate of the standard tasks based on the minimum ResponseTime and
	// the number of chunks to be loaded and unloaded per Task event
	BukkitTask unloadTask;
	int chunksPerUnload;
	BukkitTask loadTask;
	int chunksPerLoad;
	BukkitTask rescheduleTask;
	// task that periodically unloads chunks in batches
	BukkitTask unloadBatchTask;
	
	PreparedStatement addChunkStmt;
	PreparedStatement getLastChunkIdStmt;
	
	////================================================================================= ////
	
	public PlantManager(FarmPersist plugin, String databaseName, long unloadBatchPeriod, long reschedulePeriod, long minLoadTime, long maxLoadTime, long minUnloadTime, long maxUnloadTime) {
		this.plugin = plugin;
		
		chunks = new HashMap<Coords, PlantChunk>();
		
		loadChunks = new LinkedList<Coords>();
		unloadChunks = new LinkedList<Coords>();
		
		this.minLoadTime = minLoadTime;
		this.maxLoadTime = maxLoadTime;
		this.minUnloadTime = minUnloadTime;
		this.maxUnloadTime = maxUnloadTime;
		
		// open the database
		String sDriverName = "org.sqlite.JDBC";
		try {
		Class.forName(sDriverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		String sJdbc = "jdbc:sqlite";
		String sDbUrl = sJdbc + ":" + databaseName;
		int iTimeout = 30;
		
		String makeTableChunk = "CREATE TABLE IF NOT EXISTS chunk (id INTEGER PRIMARY KEY AUTOINCREMENT, w INTEGER, x INTEGER, z INTEGER)";
		String makeTablePlant = "CREATE TABLE IF NOT EXISTS plant (chunkid INTEGER, w INTEGER, x INTEGER, y INTEGER, z INTEGER, date INTEGER, growth REAL, FOREIGN KEY(chunkid) REFERENCES chunk(id))";
		
		try {
			// connect to the database
			conn = DriverManager.getConnection(sDbUrl);
			Statement stmt = conn.createStatement();
			stmt.setQueryTimeout(iTimeout);
			// make tables if they don't exist
			stmt.executeUpdate(makeTableChunk);
			stmt.executeUpdate(makeTablePlant);
				
			// load all chunks
			ResultSet rs = stmt.executeQuery("SELECT id, w, x, z FROM chunk");
			while (rs.next()) {
				int id = rs.getInt(1);
				int w = rs.getInt(2);
				int x = rs.getInt(3);
				int z = rs.getInt(4);
					
				plugin.getLogger().info("identified chunk with index "+id);
					
				PlantChunk pChunk = new PlantChunk(plugin, conn, id);
				chunks.put(new Coords(w,x,0,z), pChunk);
			} 
			
			// create prepared statements
			addChunkStmt = conn.prepareStatement("INSERT INTO chunk (w, x, z) VALUES (?, ?, ?)");
			getLastChunkIdStmt = conn.prepareStatement("SELECT last_insert_rowid()");
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		// create unload batch
		batchUnload = new ArrayList<Coords>();
		
		// start with only 1 dequeue per task callback
		chunksPerUnload = 1;
		chunksPerLoad = 1;
		
		//register the loadDequeueTAsk
		loadTask = plugin.getServer().getScheduler().runTaskTimer(plugin, new Runnable() {
		    @Override  
		    public void run() {
		        loadDequeue();
		    }
		}, minLoadTime, minLoadTime);
		
		//register the unloadDequeueTask
		unloadTask = plugin.getServer().getScheduler().runTaskTimer(plugin, new Runnable() {
		    @Override  
		    public void run() {
		        unloadDequeue();
		    }
		}, minUnloadTime, minUnloadTime);
		
		//register the rescheduleTask
		rescheduleTask = plugin.getServer().getScheduler().runTaskTimer(plugin, new Runnable() {
		    @Override  
		    public void run() {
		        reschedule();
		    }
		}, reschedulePeriod, reschedulePeriod);
		
		//register the batchTask
		unloadBatchTask = plugin.getServer().getScheduler().runTaskTimer(plugin, new Runnable() {
		    @Override  
		    public void run() {
				unloadBatch();
		    }
		}, unloadBatchPeriod, unloadBatchPeriod);
	}
	
	////================================================================================= ////
	
	public void loadDequeue() {
		int c = chunksPerLoad;
		Coords coords;
		
		while (c > 0) {
			if (loadChunks.isEmpty())
				break;
			coords = loadChunks.remove();
			if (loadChunk(coords))
				c--;
		}
	}
	
	public void unloadDequeue() {
		int c = chunksPerUnload;
		Coords coords;
		
		while (c > 0) {
			if (unloadChunks.isEmpty())
				break;
			coords = unloadChunks.remove();
			if (unloadChunk(coords))
				c--;
		}		
	}
	
	public void reschedule() {
		int queueSize = loadChunks.size();
		long interval;
		if (queueSize == 0)
			interval = minLoadTime;
		else
			interval = maxLoadTime/queueSize;
		
		if (interval > minLoadTime)
			interval = minLoadTime;
		
		if (interval == 1)
			chunksPerLoad = (int) (maxLoadTime / (queueSize == 0 ? 1 : queueSize));
		
		loadTask.cancel();
		//register the loadDequeueTAsk
		loadTask = plugin.getServer().getScheduler().runTaskTimer(plugin, new Runnable() {
		    @Override  
		    public void run() {
		        loadDequeue();
		    }
		}, 0L, interval);
		
		queueSize = unloadChunks.size();
		if (queueSize == 0)
			interval = minUnloadTime;
		else
			interval = maxUnloadTime/queueSize;
		
		if (interval > minUnloadTime)
			interval = minUnloadTime;
		
		if (interval == 1)
			chunksPerUnload = (int) (queueSize /maxUnloadTime);
		
		unloadTask.cancel();
		//register the unloadDequeueTask
		unloadTask = plugin.getServer().getScheduler().runTaskTimer(plugin, new Runnable() {
		    @Override  
		    public void run() {
		        unloadDequeue();
		    }
		}, 0L, interval);
	}
	
	private void unloadBatch() {
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		for (Coords batchCoords:batchUnload) {
			unloadfromBatch(batchCoords);

		}
		
		try {
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		batchUnload.clear();		
	}
	
	//// ================================================================================= ////
	
	public void minecraftChunkLoaded(Coords coords) {
		// if the pChunk does not exist, there is nothing to load
		if (!chunks.containsKey(coords))
			return;
	
		PlantChunk pChunk = chunks.get(coords);
		
		// if the pChunk is already loaded then it should stay loaded -- do nothing
		if (pChunk.isLoaded())
			return;
		
		loadChunks.add(coords);
	}
	
	public void minecraftChunkUnloaded(Coords coords) {
		// if the pChunk does not exist, there is nothing to unload
		if (!chunks.containsKey(coords))
			return;
		
		PlantChunk pChunk = chunks.get(coords);
		// if the pChunk is already unloaded then it should stay unloaded -- do nothing
		if (!pChunk.isLoaded())
			return;
		
		unloadChunks.add(coords);		
	}
	
	////================================================================================= ////
	
	// load the specified chunk, return true if the pChunk is actually loaded
	private boolean loadChunk(Coords coords) {
		// if the specified chunk does not exist, then don't load anything
		if (!chunks.containsKey(coords))
			return false;
		
		// this getWorlds().get(index) could break in the future
		// if the minecraft chunk is unloaded again, then don't load the pChunk
		
		if (!plugin.getServer().getWorld(WorldID.getMCID(coords.w)).isChunkLoaded(coords.x, coords.z))
			return false;
		
		// finally, just load this thing!
		PlantChunk pChunk = chunks.get(coords);
		
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		pChunk.load(conn, coords);
		try {
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return true;
	}
	
	// unload the specified chunk, return true if the pChunk is actually loaded
	private boolean unloadChunk(Coords coords) {
		// if the specified chunk does not exist, then don't unload anything
		if (!chunks.containsKey(coords))
			return false;
		
		// if the minecraft chunk is loaded again, then don't unload the pChunk
		if (plugin.getServer().getWorld(WorldID.getMCID(coords.w)).isChunkLoaded(coords.x, coords.z))
			return false;
		
		// if this chunk is already set to unload, then stop, it doesn't need to be added to
		// the unload batch
		if (batchUnload.contains(coords))
			return true;
		
		// finally, add the plant chunk's coords to be 
		// commited to the database in a batch
		batchUnload.add(coords);
		
		return true;
	}
	
	private void unloadfromBatch(Coords coords) {
		// if the specified chunk does not exist, then don't unload anything
		if (!chunks.containsKey(coords))
			return;
		
		// if the minecraft chunk is loaded again, then don't unload the pChunk
		if (plugin.getServer().getWorld(WorldID.getMCID(coords.w)).isChunkLoaded(coords.x, coords.z))
			return;
		
		// finally, actaully unload this thing
		PlantChunk pChunk = chunks.get(coords);		
		pChunk.unload(conn);
	}
	
	////================================================================================= ////
	
	public void saveAll() {
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		for (Coords coords:chunks.keySet()) {
			PlantChunk pChunk = chunks.get(coords);
			pChunk.unload(conn);
		}
		
		try {
			conn.commit();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		loadChunks = null;
		unloadChunks = null;
		
		batchUnload = null;
		
		loadTask.cancel();
		unloadTask.cancel();
		rescheduleTask.cancel();
	}
	
	public void add(Coords coords, Plant plant) {
		Coords chunkCoords = new Coords(coords.w, coords.x/16, 0, coords.z/16);
		
		plugin.getLogger().info("coords = "+coords);
		plugin.getLogger().info("chunkCoords = "+chunkCoords);
		
		PlantChunk pChunk = null;
		if (!chunks.containsKey(chunkCoords)) {
			try {
			addChunkStmt.setInt(1, chunkCoords.w);
			addChunkStmt.setInt(2, chunkCoords.x);
			addChunkStmt.setInt(3, chunkCoords.z);
			addChunkStmt.execute();
			getLastChunkIdStmt.execute();
			ResultSet rs = getLastChunkIdStmt.getResultSet();
			int chunkid = rs.getInt(1);
			plugin.getLogger().info("adding pChunk "+chunkid);
			pChunk = new PlantChunk(plugin, conn, chunkid);
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
		else
			pChunk = chunks.get(chunkCoords);
		
		if (pChunk == null)
			return;
		
		pChunk.add(coords, plant);
		chunks.put(chunkCoords, pChunk);
	}
	
	public Plant get(Coords coords) {
		Coords chunkCoords = new Coords(coords.w, coords.x/16, 0, coords.z/16);
		PlantChunk pChunk = chunks.get(chunkCoords);
		
		if (pChunk == null)
			return null;
		
		return pChunk.get(coords);
	}
	
	public void remove(Coords coords) {
		Coords chunkCoords = new Coords(coords.w, coords.x/16, 0, coords.z/16);
		PlantChunk pChunk = chunks.get(chunkCoords);
		
		if (pChunk == null)
			return;
		
		pChunk.remove(coords);		
	}
}
