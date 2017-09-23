/*
 * @author Shubham Gupta
 */

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings({ "rawtypes", "unused" })
class dproc {

	// Common Variables and Data Storage.
	static String coordinator = "";
	static String hostname = "";
	static int interval;
	static int terminate;
	static int no_process;
	static boolean is_coord = false;

	// Coordinator only data.
	static int active_process = 0;
	static int ready_process = 1;
	static Map<String, List<String>> nebr_list = new HashMap<String, List<String>>();
	static Map<String, String> id_hostnames = new HashMap<String, String>();

	// Individual Process Variables.
	static int clock = 0;
	static String id = "";
	static boolean recvd_id = false;
	static boolean recvd_names = false;
	static int hello_count = 0;
	static boolean recvd_compute = false;
	static int[] Sent;
	static boolean reg_complete = false;
	static int[] Recvd;
	static Socket socket;
	static List<String> my_nebr = new ArrayList<String>();
	static Map<String, String> my_nebr_hostnames = new HashMap<String, String>();

	/****************************** MAIN FUNCTION *********************************************/

	public static void main(String[] args) throws Exception {

		if (args.length != 0)
			if (args[0].equalsIgnoreCase("-c")) {
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
			if (!next.isEmpty()) {
				list = next.split("\\s");
				if (list[0].equalsIgnoreCase("coordinator")) {
					coordinator = list[1];
					if (is_coord)
						id_hostnames.put("1", coordinator + ".utdallas.edu");
				} else if (list[0].equalsIgnoreCase("number")) {
					no_process = Integer.valueOf(list[3]);
				} else if (list[0].equalsIgnoreCase("interval")) {
					interval = Integer.valueOf(list[1]);
				} else if (list[0].equalsIgnoreCase("terminate")) {
					terminate = Integer.valueOf(list[1]);
				} else {
					if (!list[0].equalsIgnoreCase("neighbor")) {
						String id = list[0];
						String[] nebr = Arrays
								.copyOfRange(list, 1, list.length);
						List<String> neighbour = Arrays.asList(nebr);
						nebr_list.put(id, neighbour);
						if (is_coord) {
							if (id.equalsIgnoreCase("1"))
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
		System.out.println("");

		// If the node is the coordinator then start listener.
		Thread start = new Thread() {
			public void run() {
				try {
					if (is_coord) {
						coord_register_new();

						// If the node is not the coordinator send register.
					} else {
						process_register();
						recv_msg();
					}
					System.out.println("Register thread exited");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		start.start();

		// Implement the working of the node.
		if (reg_complete) {
			while (clock < terminate) {
				System.out.println("CLOCK STARTED");
				clock++; // Increment the logical clock of the Process.

			}
		}

	}

	// Send a message to another process.
	public static void send_msg(String host, int port, String sendMessage) {
		try {

			InetAddress address = InetAddress.getByName(host);
			socket = new Socket(address, port);
			socket.setReuseAddress(true);
			// Send the message to the host
			OutputStream os = socket.getOutputStream();
			OutputStreamWriter osw = new OutputStreamWriter(os);
			BufferedWriter bw = new BufferedWriter(osw);

			bw.write(sendMessage);
			bw.flush();
			socket.shutdownOutput();

		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
			// Closing the socket
			try {
				socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// A message is received through a channel from another process.
	@SuppressWarnings("resource")
	public static void recv_msg() throws IOException {

		String message = "";
		try {

			int port = 25555;		
			ServerSocket serverSocket = new ServerSocket(port);
			System.out.println("Process listening to the port 25555");

			// Process is listening for new messages after registeration.
			while (true) {

				// Reading the message from the other process.
				socket = serverSocket.accept();
				socket.setReuseAddress(true);
				InputStream is = socket.getInputStream();
				String hostName = socket.getInetAddress().getHostName();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				message = br.readLine();
				System.out.println("Message received from " + hostName + " is "
						+ message);
				msg_type(message);

			}
		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
			// Closing the socket
			try {
				socket.close();				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	// Perform some action depending on the type of the message received.
	public static void msg_type(String message) throws IOException {
		// On Receiving a hostnames from store in map.
		if (message.split(" ")[0].equalsIgnoreCase("hostnames")) {
			String[] list = message.split(" ");
			for (int i = 1; i < list.length - 1; i++) {
				String id = list[i];
				i++;
				if (!list[i].isEmpty()) {
					my_nebr_hostnames.put(id, list[i]);
				}
			}
			reg_complete = true;
			System.out.print(my_nebr_hostnames);
			System.out.println("");
			Thread one = new Thread() {
				public void run() {
					send_hello();
				}
			};
			one.start();
		}

		// On Receiving a hello message from a channel store count.
		// Send ready if all hello messages received.
		if (message.equalsIgnoreCase("hello")) {
			hello_count++;
			if (hello_count == my_nebr.size()) {
				Thread one = new Thread() {
					public void run() {
						send_ready();
					}
				};
				one.start();
			}
		}

		// On receiving ready messages.
		if (is_coord) {
			if (message.equalsIgnoreCase("ready")) {
				ready_process++;
				if (ready_process == no_process) {
					Thread one = new Thread() {
						public void run() {
							start_compute();
							compute();
						}
					};
					one.start();
				}
			}
		}
		// On Receiving compute message.
		if (message.equalsIgnoreCase("compute")) {
			compute();
		}
		// On Receiving marker record the state.
		if (message.equalsIgnoreCase("marker")) {
			int number = Integer.valueOf(message);
			record(number);
		}
	}

	// Send the inital register message to the coordinator
	// if the process itself is not the coordinator.
	public static void process_register() throws IOException {

		if (!is_coord) {
			String msg = "REGISTER";
			int port = 25555;
			String host = coordinator + ".utdallas.edu";
			try {
				socket.setReuseAddress(true);	
				InetAddress address = InetAddress.getByName(host);
				socket = new Socket(address, port);

				// Send the message to the host
				OutputStream os = socket.getOutputStream();
				OutputStreamWriter osw = new OutputStreamWriter(os);
				BufferedWriter bw = new BufferedWriter(osw);

				bw.write(msg);
				bw.flush();
				System.out.println("Message " + msg + " sent to the : " + host);
				socket.shutdownOutput();

				// Get the return message from the coordinator.
				InputStream is = socket.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);

				String message = br.readLine();
				System.out.println("Message received : " + message);
				if (message != null) {
					String[] msg_list = message.split(" ");
					if (msg_list[0].equalsIgnoreCase("ack")) {

						// Check the id assigned and store the corresponding
						// values.
						id = msg_list[1];
						System.out.println("Id received : " + id);
						String[] nebr = Arrays.copyOfRange(msg_list, 2,
								msg_list.length);
						List<String> neighbour = Arrays.asList(nebr);
						my_nebr = neighbour;
						System.out.print(my_nebr);
						System.out.println("");
					}
				}

			} catch (Exception exception) {
				exception.printStackTrace();
			} finally {
				// Closing the socket
				try {
					socket.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	// Processing the register requests received from
	// non-coordinator processes.
	@SuppressWarnings("resource")
	public static void coord_register_new() throws IOException {

		try {
			
			int port = 25555;
			ServerSocket serverSocket = new ServerSocket(port);
			System.out.println("Coordinator listening to the port 25555");

			// Coordinator is listening always for new process registering.
			while (true) {

				// Reading the message from the other process.
				socket = serverSocket.accept();
				socket.setReuseAddress(true);
				String hostName = socket.getInetAddress().getHostName();
				InputStream is = socket.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				String message = br.readLine();
				System.out.println("Message from " + hostName + " is "
						+ message);

				// Sending the process its id.
				if (message.equalsIgnoreCase("register")) {
					String returnMessage;
					if (active_process < no_process) {

						// Assign a id to the process and send it the
						// information.
						// Also store its hostname.
						active_process++;
						String id = Integer.toString(active_process);
						returnMessage = "ACK " + id;
						List<String> temp = nebr_list.get(id);
						for (String s : temp) {
							returnMessage = returnMessage + " " + s;
						}

						// Store the id and the hostname in the map.
						id_hostnames.put(id, hostName);

						// Sending the response back to the process.
						OutputStream os = socket.getOutputStream();
						OutputStreamWriter osw = new OutputStreamWriter(os);
						BufferedWriter bw = new BufferedWriter(osw);
						bw.write(returnMessage);
						System.out
								.println("Message sent is : " + returnMessage);
						bw.flush();
						socket.shutdownOutput();
					}
				} else {
					msg_type(message);
				}

				if (active_process == no_process && !reg_complete) {
					Thread send = new Thread() {
						public void run() {
							try {
								send_hostnames();
								send_hello();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					};
					send.start();
					reg_complete = true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				socket.close();
			} catch (Exception e) {
			}
		}
	}

	// Send the hostnames of the neighbours to registered processes
	// once all processes have registered and their hostnames are stored.
	public static void send_hostnames() throws IOException {

		// If all processes sent the register message and coordinator
		// knows everyone's hostname.

		if (active_process == no_process) {

			// Coordinator registers its own neighbours
			if (is_coord) {
				for (int i = 0; i < nebr_list.get("1").size(); i++) {
					String id = nebr_list.get("1").get(i);
					String host = (String) id_hostnames.get(id);
					my_nebr_hostnames.put(id, host);
				}
			}

			// Send hostnames of neightbours to processes.
			for (int i = 2; i <= no_process; i++) {
				String message = "Hostnames";
				int size = nebr_list.get("" + i).size();
				for (int j = 0; j < size; j++) {
					String id = nebr_list.get("" + i).get(j);
					String host = (String) id_hostnames.get(id);
					message = message + " " + id + " " + host;
				}
				send_msg(id_hostnames.get("" + i), 25555, message);
			}
		}
	}

	// Send a hello message to all the neighbours.
	public static void send_hello() {

		Iterator it = my_nebr_hostnames.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String id = (String) pair.getKey();
			String host = (String) pair.getValue();
			String msg = "HELLO";
			send_msg(host, 25555, msg);
		}
	}

	// Send a ready message to coordinator after verifying all hello messages.
	public static void send_ready() {
		if (!is_coord) {
			String msg = "READY";
			send_msg(coordinator, 25555, msg);
			System.out.println("Sent READY to the coordinator.");
		}
	}

	// Send the compute message after receiving all ready messages.
	public static void start_compute() {

		if (ready_process == no_process) {
			Iterator it = id_hostnames.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				String id = (String) pair.getKey();
				String host = (String) pair.getValue();
				String msg = "COMPUTE";
				send_msg(host, 25555, msg);
			}
		}
	}

	// Simulates the computing process of a node on receiving a message.
	public static void compute() {
		try {
			// Randomly sleep the thread for upto 5ms.
			Thread.sleep((long) (Math.random() * 5000));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// After wakingup select a random number between 0,1,2.
		int randomNum = ThreadLocalRandom.current().nextInt(0, 2 + 1);

		for (int i = 0; i < randomNum; i++) {
			int max = my_nebr.size();

			// Randomly choose a node from the list of neighbours.
			int random = ThreadLocalRandom.current().nextInt(0, max + 1);
			String id = my_nebr.get(random);

			// Send a message to the node with that id.
			String message = "COMPUTE";
			send_msg(my_nebr_hostnames.get(id), 25555, message);
		}
	}

	// Start the snapshot algorithm.
	public static void initiate() {
		if (clock % interval == 0) {
			Iterator it = my_nebr_hostnames.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				String id = (String) pair.getKey();
				String host = (String) pair.getValue();
				String msg = "MARKER";
				send_msg(host, 0, msg);
			}
		}
	}

	// Recording states upon receiving a marker.
	public static void record(int number) throws IOException {
		String name = "Localstate_" + id;
		BufferedWriter WriteFile = new BufferedWriter(
				new FileWriter(name, true));
		WriteFile.write("Recording Number: " + number);
		WriteFile.write('\n');
		WriteFile.write("Sent: " + Sent.toString());
		WriteFile.write('\n');
		WriteFile.write("Recvd: " + Recvd.toString());
		WriteFile.write('\n');
		WriteFile.close();
	}

}