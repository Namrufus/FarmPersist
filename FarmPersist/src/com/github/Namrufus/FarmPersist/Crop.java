package com.github.Namrufus.FarmPersist;

import java.util.ArrayList;
import java.util.HashMap;

import org.bukkit.Material;
import org.bukkit.block.Block;
import org.bukkit.block.Biome;
import org.bukkit.plugin.Plugin;

public class Crop {
	// the crop block material
	private Material material;
	
	// the base growth time in growth/ms
	private float baseGrowthRate;
	
	// if this crop's growth rate is modulated by sunlight or not
	private boolean needsSunlight;
	
	// stunt or accelerate the base growth rate by biome
	private HashMap<Biome, Float> biomes;
	
	// bonus added if the block is exposed directly to the sky.
	private float skyBonus;
	
	// list of block layers that can add a bonus to the growth rate
	private ArrayList<Soil> soils;
	
	// array of different amounts of soils that can be added under the farmland block in order
	// to add a bonus per block
	public class Soil {
		// the material that gives the bonus
		public Material material;
		// the bonus per underlying block
		public float bonus;
		// the maximum amount of soil for this layer
		public int maxAmount;
		
		public Soil(Material material, float bonus, int maxAmount) {
			this.material = material;
			this.bonus = bonus;
			this.maxAmount = maxAmount;
		}
	}
	
	public Crop (String matName, Plugin plugin) {
		String key = "FarmPersist.crops."+matName;
		
		material = Material.getMaterial(matName);
		if (material == null)
			plugin.getLogger().warning("while loading crop configs, "+matName+"is not a valid material.");
			
		// baseRate is stored as the growth time in days so it needs to be converted into 
		// growth per millisecond
		baseGrowthRate = (float) plugin.getConfig().getDouble(key+".baseRate");
		baseGrowthRate = (float) (1.0/(1000.0*60.0*60.0*24.0*baseGrowthRate));
			
		needsSunlight = plugin.getConfig().getBoolean(key+".needsSunlight");
			
		skyBonus =  (float) plugin.getConfig().getDouble(key+".skyBonus");
			
		biomes = new HashMap<Biome, Float>();
			
		// get all biomes
		for (String biomeName:plugin.getConfig().getConfigurationSection(key+".biomes").getKeys(false)) {
			String key2 = key+".biomes."+biomeName;
			Biome biome = Biome.valueOf(biomeName);
			if (biome == null)
				plugin.getLogger().warning("while loading crop configs, "+biomeName+"is not a valid biome.");
			float amount = (float) plugin.getConfig().getDouble(key2);
			biomes.put(biome, amount);
		}
			
		// get all soils
		soils = new ArrayList<Soil>();
		for (String soilName:plugin.getConfig().getConfigurationSection(key+".soil").getKeys(false)) {
			String key2 = key+".soil."+soilName;
			Material sMat = Material.getMaterial(soilName);
			if (sMat == null)
				plugin.getLogger().warning("while loading crop configs, "+soilName+"is not a valid material.");
			float sBonus = (float) plugin.getConfig().getDouble(key2+".bonus");
			int sMax = plugin.getConfig().getInt(key2+".max");
			soils.add(new Soil(sMat, sBonus, sMax));
		}
	}
	
	public Material getMaterial() {
		return material;
	}
	
	// get the amount that this crop has grown as a fraction of total maturity
	public float getGrowth(Block block, float time) {
		float rate = baseGrowthRate;
		
		// modulate the rate by the amount that the 
		if (needsSunlight) {
			rate *= block.getLightFromSky() / 15.0;
		}
		
		Float biomeMultiplier = biomes.get(block.getBiome());
		if (biomeMultiplier != null)
			rate *= biomeMultiplier.floatValue();
		
		if (skyBonus != 0.0) {
			Block newBlock = block.getRelative(0, 1, 0);
			int y = 0;
			while (newBlock != null && newBlock.getType() == Material.AIR) {
				y = newBlock.getY();
				newBlock = newBlock.getRelative(0, 1, 0);
			}
			
			if (y == 255)
				rate *= skyBonus;
		}
		
		float soilBonus = 0.0f;
		Block newBlock = block.getRelative(0,-2,0);
		for (Soil soil:soils) {
			if (newBlock == null)
				break;
			
			int soilCount = 0;
			while (soilCount < soil.maxAmount) {
				if (newBlock == null || !newBlock.getType().equals(soil.material))
					break;
				
				soilBonus += soil.bonus;
				
				newBlock = newBlock.getRelative(0, -1, 0);
				soilCount++;
			}
		}
		rate *= (1.0 + soilBonus);
		
		return rate*time;
	}
}
