/*
 * @author Shubham Gupta
 */

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


class dproc {

	// Common Variables and Data Storage.
	static String coordinator = "";
	static String hostname = "";
	static int interval;
	static int terminate;
	static int no_process;
	static boolean is_coord = false;
	static Map<String, List<String>> nebr_list = new HashMap<String, List<String>>();
	
	// Coordinator only data.
	static int active_process = 0;
	static Map<String,String> list_hostnames = new HashMap<String, String>();
	
	// Individual Process Variables.
	static int clock = 0;
	static String id = "";
	static boolean recvd_id = false;
	static boolean recvd_names = false;
	static boolean recvd_compute = false;
	static List<String> my_nebr = new ArrayList<String>();
		
	
	/****************************** MAIN FUNCTION *********************************************/

	public static void main(String[] args) throws Exception {

		if(args.length != 0)
			if(args[0].equalsIgnoreCase("-c")) {
				is_coord = true;
				id = "1";
				active_process++;
			}
						
		// Read the dsConfig File and assign the data to proper variables.
		Scanner sc2 = null;
		String[] list = null;
		try {
			sc2 = new Scanner(new File("dsConfig"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		while (sc2.hasNextLine()) {
			String next = sc2.nextLine();
			if(!next.isEmpty()) {
				list = next.split("\\s");
				if(list[0].equalsIgnoreCase("coordinator")){
					coordinator = list[1];
					if(coordinator.equalsIgnoreCase(hostname))
						is_coord = true;
				} else
				if(list[0].equalsIgnoreCase("number")){
					no_process = Integer.valueOf(list[3]);
				} else 
				if(list[0].equalsIgnoreCase("interval")){
					interval = Integer.valueOf(list[1]);
				} else 
				if(list[0].equalsIgnoreCase("terminate")){
					terminate = Integer.valueOf(list[1]);
				} else {
					if(!list[0].equalsIgnoreCase("neighbor")){
						String id = list[0];
						List<String> neighbour = Arrays.asList(list);
						nebr_list.put(id, neighbour);
						if(is_coord) {
							if(id.equalsIgnoreCase("1"))
								my_nebr = neighbour;							
						}	
					}
				}
			}
		}
		System.out.println(coordinator);
		System.out.println(is_coord);
		System.out.println(no_process);
		System.out.println(interval);
		System.out.println(terminate);
		System.out.print(nebr_list);
		
		// Implement the working of the node.
		while(clock < terminate) {
			clock++;
			
			// If the node is the coordinator
			if(is_coord){
				
			// If the node is not the coordinator.	
			} else {
				send_register();
			}
		}
	}
	
	public static void send_msg(){
		
	}
	
	public static void recv_msg(){
		
	}
	
	public static void send_register(){
		
	}
	
	public static void recv_register(){
		
	}
	
	public static void send_hostnames(){
		
	}
	
	public static void send_hello() {
		
	}
	
	public static void compute(){
		try {
			Thread.sleep((long)(Math.random() * 5000));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}
	
}