package com.github.Namrufus.FarmPersist;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.bukkit.Chunk;
import org.bukkit.World;
import org.bukkit.block.Block;

public class PlantChunk {
	FarmPersist plugin;
	
	HashMap<Coords,Plant> plants;
	
	// index of this chunk in the database
	int index;
	
	boolean loaded;
	
	private static PreparedStatement deleteOldDataStmt = null;
	private static PreparedStatement loadPlantsStmt = null;
	private static PreparedStatement deleteChunkStmt = null;
	private static PreparedStatement savePlantsStmt = null;
	
	public PlantChunk(FarmPersist plugin, Connection conn, int index) {
		this.plugin = plugin;
		plants = null;
		this.index = index;
		
		this.loaded = false;

		if (deleteOldDataStmt == null) {
			try {
			deleteOldDataStmt = conn.prepareStatement("DELETE FROM plant WHERE chunkid = ?1");
			loadPlantsStmt = conn.prepareStatement("SELECT w, x, y, z, date, growth FROM plant WHERE chunkid = ?1");
			savePlantsStmt = conn.prepareStatement("INSERT INTO plant (chunkid, w, x, y, z, date, growth) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)");
			deleteChunkStmt = conn.prepareStatement("DELETE FROM chunk WHERE id = ?1");
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	///-------------------------
	
	public boolean isLoaded() {
		return loaded;
	}
	
	public int getPlantCount() {
		return plants.keySet().size();
	}
	
	///-------------------------
	
	public void remove(Coords coords) {
		if (!loaded)
			return;
		
		plants.remove(coords);
	}
	
	public void add(Coords coords, Plant plant) {
		if (!loaded) {
			plants = new HashMap<Coords, Plant>();
			loaded = true;
		}
		
		plants.put(coords, plant);
	}
	
	public Plant get(Coords coords) {
		if (!loaded)
			return null;
		return plants.get(coords);
	}

	public void load(Connection conn, Coords coords) {
		if (loaded)
			return;
		
		Chunk chunk = null;
		World world = plugin.getServer().getWorld(WorldID.getMCID(coords.w));
		if (world.isChunkLoaded(coords.x, coords.z))
			chunk = world.getChunkAt(coords.x, coords.z);

		plants = new HashMap<Coords, Plant>();
		
		try {
			loadPlantsStmt.setInt(1, index);
			loadPlantsStmt.execute();
			ResultSet rs = loadPlantsStmt.getResultSet();
			while (rs.next()) {
				int w = rs.getInt(1);
				int x = rs.getInt(2);
				int y = rs.getInt(3);
				int z = rs.getInt(4);
				long date = rs.getLong(5);
				float growth = rs.getFloat(6);
			
				// if the plant does not correspond to an actual crop, don't load it
				if (chunk != null && !plugin.getCrops().containsKey(chunk.getBlock(x, y, z).getType())) {
					continue;
				}
					
				Plant plant = new Plant(date,growth);
					
				// grow the block
				Block block = chunk.getBlock(x, y, z);
				Crop crop = plugin.getCrops().get(block.getType());
				float growthAmount = crop.getGrowth(block, plant.setUpdateTime(System.currentTimeMillis()));
				plant.addGrowth(growthAmount);
					
				// and update its block
				plugin.growBlock(block,coords,plant.getGrowth());
					
				// if the plant isn't finished growing, add it to the 
				// plants
				if (!(plant.getGrowth() >= 1.0))
					plants.put(new Coords(w,x,y,z), plant);
			} 			
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		loaded = true;
	}
	
	public void unload(Connection conn) {
		if (!loaded)
			return;
		
		try {
			// first, delete the old data
			deleteOldDataStmt.setInt(1, index);
			deleteOldDataStmt.execute();
			
			// then replace it with all the recorded plants in this chunk
			if (!plants.isEmpty()) {
				for (Coords coords: plants.keySet()) {
					Plant plant = plants.get(coords);
					
					savePlantsStmt.setInt(1, index);
					savePlantsStmt.setInt(2, coords.w);
					savePlantsStmt.setInt(3, coords.x);
					savePlantsStmt.setInt(4, coords.y);
					savePlantsStmt.setInt(5, coords.z);
					savePlantsStmt.setLong(6, plant.getUpdateTime());
					savePlantsStmt.setFloat(7, plant.getGrowth());
					
					savePlantsStmt.execute();
				}
			}
			else {
				// otherwise just delete the chunk entirely
				deleteChunkStmt.setInt(1, index);
				deleteChunkStmt.execute();
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		plants = null;
		loaded = false;
	}
}
