package com.github.Namrufus.FarmPersist;

import java.util.UUID;

public class WorldID {
	private static UUID overworldID;
	private static UUID netherID;
	private static UUID endID;
	
	public static void init (UUID overworldID, UUID netherID, UUID endID) {
		WorldID.overworldID = overworldID;
		WorldID.netherID = netherID;
		WorldID.endID = endID;
	}
	
	public static UUID getMCID(int id) {
		if (id == 0)
			return overworldID;
		else if (id == 1)
			return netherID;
		else if (id == 2)
			return endID;
		else
			return null;
	}
	
	public static int getPID(UUID id) {
		if (id == overworldID)
			return 0;
		else if (id == netherID)
			return 1;
		else if (id == endID)
			return 2;
		else
			return -1;		
	}
}
