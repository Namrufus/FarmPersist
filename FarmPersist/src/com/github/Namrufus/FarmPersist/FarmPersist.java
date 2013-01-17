package com.github.Namrufus.FarmPersist;


import java.util.HashMap;

import org.bukkit.Chunk;
import org.bukkit.Material;
import org.bukkit.block.Block;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.Action;
import org.bukkit.event.block.BlockGrowEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.world.ChunkLoadEvent;
import org.bukkit.event.world.ChunkUnloadEvent;
import org.bukkit.plugin.java.JavaPlugin;

public final class FarmPersist extends JavaPlugin implements Listener {

	private PlantManager plantManager;
	private HashMap<Material, Crop> crops;
	
	@Override
	public void onEnable() {
		WorldID.init(this.getServer().getWorld("world").getUID(),
					 this.getServer().getWorld("world_nether").getUID(),
					 this.getServer().getWorld("world_the_end").getUID());
		
		String filePath = this.getConfig().getString("FarmPersist.filePath");
		int minLoadTime = this.getConfig().getInt("FarmPersist.minLoadTime");
		int maxLoadTime = this.getConfig().getInt("FarmPersist.maxLoadTime");
		int minUnloadTime = this.getConfig().getInt("FarmPersist.minUnloadTime");
		int maxUnloadTime = this.getConfig().getInt("FarmPersist.maxUnloadTime");
		int rescheduleTime = this.getConfig().getInt("FarmPersist.reschedulePeriod");
		int unloadBatchTime = this.getConfig().getInt("FarmPersist.unloadBatchTime");
		
		plantManager = new PlantManager(this, filePath, unloadBatchTime, rescheduleTime, minLoadTime, maxLoadTime, minUnloadTime, maxUnloadTime);
		
		crops = new HashMap<Material, Crop>();
		
		for (String matName: getConfig().getConfigurationSection("FarmPersist.crops").getKeys(false)) {
			Crop crop = new Crop(matName, this);
			crops.put(crop.getMaterial(), crop);
		}
		
		getServer().getPluginManager().registerEvents(this, this);
	}
	
	@Override
	public void onDisable() {
		plantManager.saveAll();
		plantManager = null;
	}
	
	@EventHandler
	public void onChunkLoad(ChunkLoadEvent e) {
		Chunk chunk = e.getChunk();
		int w = WorldID.getPID(e.getChunk().getWorld().getUID());
		Coords coords = new Coords(w, chunk.getX(), 0, chunk.getZ());
		plantManager.minecraftChunkLoaded(coords);
	}
	
	@EventHandler
	public void onChunkUnload(ChunkUnloadEvent e) {
		Chunk chunk = e.getChunk();
		int w = WorldID.getPID(e.getChunk().getWorld().getUID());
		Coords coords = new Coords(w, chunk.getX(), 0, chunk.getZ());
		plantManager.minecraftChunkUnloaded(coords);
	}
	
	@EventHandler
	public void onBlockGrow(BlockGrowEvent e) {
		Material cropMat = e.getBlock().getType();
		
		// check if we need to intervene
		Crop crop = crops.get(cropMat);
		if (crop == null)
			return;

		Block block = e.getBlock();
		
		int w = WorldID.getPID(block.getWorld().getUID());
		Coords coords = new Coords(w, block.getX(), block.getY(), block.getZ());
		Plant plant = plantManager.get(coords);
		
		if (plant == null) {
			plant = new Plant(System.currentTimeMillis());
			plantManager.add(coords, plant);
		}
		else {
			float growthAmount = crop.getGrowth(e.getBlock(), plant.setUpdateTime(System.currentTimeMillis()));
			plant.addGrowth(growthAmount);
		}
		
		growBlock(block,coords,plant.getGrowth());
		
		// cancel the event as the growth amount has already been specified
		e.setCancelled(true);
	}
	
	public void growBlock(Block block, Coords coords, float growth) {
		block.setData((byte)(7.0*growth));
		
		// if the plant is finished growing, then remove it from the manager
		if (growth >= 1.0) {
			block.setData((byte) 7);
			plantManager.remove(coords);
		}		
	}
	
	@EventHandler
	public void onBlockPlace(BlockPlaceEvent event) {
		// if the block placed was a recognized crop, register it with the manager
		Block block = event.getBlockPlaced();
		Crop crop = crops.get(block.getType());
		if (crop == null)
			return;	
		
		int w = WorldID.getPID(block.getWorld().getUID());
		plantManager.add(new Coords(w, block.getX(), block.getY(), block.getZ()), new Plant(System.currentTimeMillis()));
	}
	
	@EventHandler
	public void onPlayerInteractEvent(PlayerInteractEvent event) {
		// right click block with the seeds or plant in hand to see what the status is
		if (event.getAction() == Action.LEFT_CLICK_BLOCK) {
			Material cMat = Material.AIR;
			if (event.getMaterial() == Material.CARROT_ITEM)
				cMat = Material.CARROT;
			if (event.getMaterial() == Material.SEEDS)
				cMat = Material.CROPS;
			if (event.getMaterial() == Material.POTATO_ITEM)
				cMat = Material.POTATO;
			if (event.getMaterial() == Material.NETHER_WARTS)
				cMat = Material.NETHER_STALK;
			
			Crop crop = crops.get(cMat);
			if (crop == null)
				return;
			
			float growthAmount = crop.getGrowth(event.getClickedBlock().getRelative(0,1,0), 1.0f);
			growthAmount = (float) (1.0/(1000.0*60.0*60.0*24.0*growthAmount));
			
			event.getPlayer().sendMessage("days till growth: "+growthAmount);
		}
	}
	
	public HashMap<Material, Crop> getCrops() {
		return crops;
	}
}
