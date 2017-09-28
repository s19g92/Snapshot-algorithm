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
	static int ready_process = 0;
	static int marker_no = 0;
	static boolean f_sent = false;
	static Map<String, List<String>> nebr_list = new HashMap<String, List<String>>();
	static Map<String, String> id_hostnames = new HashMap<String, String>();

	// Individual Process Variables.
	static int clock = 0;
	static String id = "";
	static boolean recvd_id = false;
	static boolean recvd_names = false;
	static int hello_count = 0;
	static boolean recvd_compute = false;
	static boolean reg_complete = false;
	static int[] Sent;
	static int[] Recvd;
	static int[] Channel_state;
	static Socket socket;
	static List<String> my_nebr = new ArrayList<String>();
	static Map<String, String> my_nebr_hostnames = new HashMap<String, String>();
	static List<Integer> marker_pending = new ArrayList<Integer>();
	static List<int[]> recvd_state = new ArrayList<int[]>();
	static List<int[]> sent_state = new ArrayList<int[]>();
	static List<int[]> chn_state = new ArrayList<int[]>();
	static List<Integer> counts = new ArrayList<Integer>();
	static Map<Integer, Integer> m_count = new HashMap<Integer, Integer>();
	static boolean final_done = false;

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
							if (id.equalsIgnoreCase("1")) {
								my_nebr = neighbour;
								Sent = new int[my_nebr.size()];
								Recvd = new int[my_nebr.size()];
								Channel_state = new int[my_nebr.size()];
							}
						}
					}
				}
			}
		}
		System.out.println("Coordinator : " + coordinator);
		System.out.println("Is coordinator : " + is_coord);
		System.out.println("No. of process :" + no_process);
		System.out.println("Interval : " + interval);
		System.out.println("Terminate : " + terminate);
		System.out.print("List of Nebrs : " + nebr_list);
		System.out.println("");

		// If the node is the coordinator then start listener.
		Thread start = new Thread() {
			public void run() {
				try {
					if (is_coord) {
						coord_register_new();
						recv_msg();
						// If the node is not the coordinator send register.
					} else {
						process_register();
						recv_msg();
					}
					System.out.println("Program thread exited");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		start.start();
	}

	// Send a message to another process.
	public static void send_msg(String host, int port, String sendMessage) {
		try {

			InetAddress address = InetAddress.getByName(host);
			socket = new Socket(address, port);

			// Send the message to the host
			OutputStream os = socket.getOutputStream();
			OutputStreamWriter osw = new OutputStreamWriter(os);
			BufferedWriter bw = new BufferedWriter(osw);

			bw.write(sendMessage + "!");
			bw.flush();

		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
			// Closing the socket
			try {
				// socket.close();
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

				InputStream is = socket.getInputStream();
				String hostName = socket.getInetAddress().getHostName();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				final char endMarker = '!';

				// StringBuffer if you need to be threadsafe
				StringBuilder messageBuffer = new StringBuilder();
				int value;
				// reads to the end of the stream or till end of message
				while ((value = br.read()) != -1) {
					char c = (char) value;
					if (c == endMarker) {
						break;
					} else {
						messageBuffer.append(c);
					}
				}
				// message is complete!
				message = messageBuffer.toString();

				if (message != null) {
					System.out.println("Message received from " + hostName
							+ " is " + message);
					final String msg = message;
					Thread one = new Thread() {
						public void run() {
							try {
								msg_type(msg);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					};
					one.start();
				}
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
			// Closing the socket
			try {
				// socket.close();
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
			send_hello();
		}

		// On Receiving a hello message from a channel store count.
		// Send ready if all hello messages received.
		else if (message.equalsIgnoreCase("hello")) {
			hello_count++;
			if (hello_count == my_nebr.size()) {
				send_ready();
			}
		}

		// On receiving ready messages.
		else if (is_coord && message.equalsIgnoreCase("ready")) {
			ready_process++;
			if (ready_process == no_process) {
				start_compute();
				compute();
			}
		}

		// On Receiving compute message.
		else if (message.split(" ")[2].equalsIgnoreCase("compute")) {
			clock = Math.max(Integer.valueOf(message.split(" ")[1]), clock + 1);
			for (int i = 0; i < my_nebr.size(); i++) {
				if (message.split(" ")[0].equalsIgnoreCase(my_nebr.get(i))) {
					Recvd[i]++;
				}
			}
			compute();
		}
		// On Receiving marker record the state.
		else if (message.split(" ")[2].equalsIgnoreCase("marker")) {
			clock = Math.max(Integer.valueOf(message.split(" ")[1]), clock + 1);
			check_marker_interval();
			String msg[] = message.split(" ");
			int number = Integer.valueOf(msg[3]);
			int channel = 0;

			// Check which channel the marker is from and pass it.
			for (int i = 0; i < my_nebr.size(); i++) {
				if (msg[0].equalsIgnoreCase(my_nebr.get(i))) {
					channel = i;
					Recvd[i]++;
					break;
				}
			}

			// Check if its the final recording
			if (msg[4].equalsIgnoreCase("-f")) {
				start_record(number, true, channel);
			} else {
				start_record(number, false, channel);
			}
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

				InetAddress address = InetAddress.getByName(host);
				socket = new Socket(address, port);

				// Send the message to the host
				OutputStream os = socket.getOutputStream();
				OutputStreamWriter osw = new OutputStreamWriter(os);
				BufferedWriter bw = new BufferedWriter(osw);

				bw.write(msg + "!");
				bw.flush();
				System.out.println("Message " + msg + " sent to the : " + host);

				// Get the return message from the coordinator.
				InputStream is = socket.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);

				final char endMarker = '!';

				// StringBuffer if you need to be threadsafe
				StringBuilder messageBuffer = new StringBuilder();
				int value;
				// reads to the end of the stream or till end of message
				while ((value = br.read()) != -1) {
					char c = (char) value;
					if (c == endMarker) {
						break;
					} else {
						messageBuffer.append(c);
					}
				}
				// message is complete!
				String message = messageBuffer.toString();

				if (message != null) {
					System.out.println("Message received : " + message);
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
						Sent = new int[my_nebr.size()];
						Recvd = new int[my_nebr.size()];
						Channel_state = new int[my_nebr.size()];
						System.out.print(my_nebr);
						System.out.println("");
					}
				}

			} catch (Exception exception) {
				exception.printStackTrace();
			} finally {
				// Closing the socket
				try {
					// socket.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	// Processing the register requests received from
	// non-coordinator processes.
	public static void coord_register_new() throws IOException {

		try {

			int port = 25555;
			@SuppressWarnings("resource")
			ServerSocket serverSocket = new ServerSocket(port);
			System.out.println("Coordinator listening to the port 25555");

			// Coordinator is listening always for new process registering.
			while (true) {

				// Reading the message from the other process.
				socket = serverSocket.accept();

				String hostName = socket.getInetAddress().getHostName();
				InputStream is = socket.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				final char endMarker = '!';

				// StringBuffer if you need to be threadsafe
				StringBuilder messageBuffer = new StringBuilder();
				int value;
				// reads to the end of the stream or till end of message
				while ((value = br.read()) != -1) {
					char c = (char) value;
					if (c == endMarker) {
						break;
					} else {
						messageBuffer.append(c);
					}
				}
				String message = messageBuffer.toString();

				if (message != null) {

					// Create a new thread for every message received to process
					// it after register is complete. Else, go to register.
					if (reg_complete) {
						System.out.println("Message from " + hostName + " is "
								+ message);
						final String msg = message;
						Thread yz = new Thread() {
							public void run() {
								try {
									msg_type(msg);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						};
						yz.start();
					}

					// Sending the process its id.
					else if (message.equalsIgnoreCase("register")) {
						System.out.println("Message from " + hostName + " is "
								+ message);
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
							bw.write(returnMessage + "!");
							System.out.println("Message sent is : "
									+ returnMessage);
							bw.flush();
						}
					}
				}
				if (active_process == no_process && !reg_complete) {
					System.out
							.println("All process registered ! Sending out hostnames");
					send_hostnames();
					send_hello();
					reg_complete = true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// socket.close();
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
			System.out.println("HELLO Sent to " + host);
			send_msg(host, 25555, msg);
		}
	}

	// Send a ready message to coordinator after verifying all hello messages.
	public static void send_ready() {
		if (!is_coord) {
			String msg = "READY";
			send_msg(coordinator, 25555, msg);
			System.out.println("Sent READY to the coordinator.");
		} else {
			ready_process++;
		}
	}

	// Send the compute message after receiving all ready messages.
	public static void start_compute() {

		if (ready_process == no_process) {
			for (int x = 0; x < my_nebr.size(); x++) {
				Sent[x]++;
			}
			Iterator it = id_hostnames.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				String id_proc = (String) pair.getKey();
				String host = (String) pair.getValue();
				clock = clock + 1;
				String msg = id + " " + clock + " COMPUTE";
				if (!id_proc.equalsIgnoreCase("1")) {
					send_msg(host, 25555, msg);
					System.out.println("SENT " + msg + " to " + host);
				}
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
		int randomNum = ThreadLocalRandom.current().nextInt(1, 2 + 1);
		for (int i = 0; i < randomNum; i++) {
			clock = clock + 1;
			// if its the coordinator check if marker needs to be sent
			check_marker_interval();

			// Randomly choose a node from the list of neighbours.
			int max = my_nebr.size();
			int random = ThreadLocalRandom.current().nextInt(0, max);

			String id_nebr = my_nebr.get(random);

			if (clock < terminate) {
				Sent[random]++;

				// Send a message to the node with that id.
				String message = id + " " + clock + " COMPUTE";
				send_msg(my_nebr_hostnames.get(id_nebr), 25555, message);
				System.out.println("SENT " + message + " to " + id_nebr);
			}
		}
	}

	// Check if a marker needs to be initiated.
	public static void check_marker_interval() {
		if (is_coord) {
			if (clock >= interval && clock < terminate) {
				System.out.println("MARKER " + marker_no + " SENT AT CLOCK :"
						+ clock);
				send_marker(marker_no, false);
				marker_pending.add(marker_no);
				recvd_state.add(Recvd);
				sent_state.add(Sent);
				chn_state.add(Channel_state);
				counts.add(0);
				marker_no++;
				interval = interval + interval;

			} else if (clock >= terminate && !f_sent) {
				// Start Final Marker.
				System.out.println("FINAL MARKER " + marker_no
						+ " SENT AT CLOCK :" + clock);
				send_marker(marker_no, true);
				marker_no++;
				marker_pending.add(marker_no);
				recvd_state.add(Recvd);
				sent_state.add(Sent);
				chn_state.add(Channel_state);
				counts.add(0);
				f_sent = true;
			}
		}
	}

	// Start the snapshot algorithm.
	public static void send_marker(int i, boolean f) {
		Iterator it = my_nebr_hostnames.entrySet().iterator();
		String msg = null;

		// Increase the send count for all nebrs.
		for (int x = 0; x < my_nebr.size(); x++) {
			Sent[x]++;
		}

		// Send markers to all nebrs
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String id_send = (String) pair.getKey();
			String host = (String) pair.getValue();
			if (!f)
				msg = id + " " + clock + " MARKER " + i + " -n";
			else
				msg = id + " " + clock + " MARKER " + i + " -f";

			send_msg(host, 25555, msg);
		}
	}

	// On receving a marker, record states and forward the marker to all nebrs.
	public static void start_record(int number, boolean f, int ch)
			throws IOException {

		boolean old = false;
		if (!marker_pending.isEmpty())
			for (int num = 0; num < marker_pending.size(); num++) {
				if (num == number) {
					// Marker has been previously received. Just record channel
					// state.
					old = true;
					int[] c_count = chn_state.get(num);
					c_count[ch] = Recvd[ch] - recvd_state.get(num)[ch];
					chn_state.set(num, c_count);
					counts.set(num, counts.get(num) + 1);

					// if received markers from all channels, record count,
					// write to file and
					// move on.
					if (counts.get(num) == my_nebr.size()) {
						record(number);
						if (f)
							System.exit(0);
					}
				}
			}
		if (!old) {
			// If the marker is new and if no previous recording is pending
			// Do a new recording and send markers to all neighbours.
			marker_pending.add(number);
			recvd_state.add(Recvd);
			sent_state.add(Sent);
			chn_state.add(Channel_state);
			counts.add(1);
			send_marker(number, f);
		}
	}

	// Recording states upon receiving a marker.
	public static void record(int number) throws IOException {
		String name = "Localstate_" + id + ".txt";
		BufferedWriter WriteFile = new BufferedWriter(
				new FileWriter(name, true));
		WriteFile.write("Recording Number: " + number);
		WriteFile.write('\n');
		WriteFile.write("SENT : ");
		WriteFile.write(Arrays.toString(sent_state.get(number)));
		WriteFile.write('\n');
		WriteFile.write("RECVD : ");
		WriteFile.write(Arrays.toString(recvd_state.get(number)));
		WriteFile.write('\n');
		WriteFile.write("CHANNEL : ");
		WriteFile.write(Arrays.toString(chn_state.get(number)));
		WriteFile.write('\n');
		WriteFile.close();
	}
}