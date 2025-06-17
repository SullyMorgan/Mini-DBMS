# client_utils.py

import socket
import json

HOST = 'localhost'
PORT = 65432

def send_command_via_existing_socket(s: socket.socket, command: str) -> str:
    try:
        s.sendall((command + "\n").encode())

        buffer = ""
        while "<<END>>" not in buffer:
            data = s.recv(4096).decode()
            if not data:
                return "Error: Server disconnected unexpectedly."
            buffer += data
        
        #response, _ = buffer.split("\n", 1)
        response = buffer.replace("<<END>>", "").strip()
        return response
    except BrokenPipeError:
        return "Error: Connection to server lost (Broken Pipe)."
    except ConnectionResetError:
        return "Error: Server reset the connection."
    except Exception as e:
        return f"Error during command sending/receiving: {e}"

def send_batch_commands_new_socket(commands: list[str]) -> list[str]:
    responses = []
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((HOST, PORT))
            
            full_command_string = "\n".join(commands) + "\n" 
            s.sendall(full_command_string.encode())
            
            recv_buffer = ""
            expected_responses = len(commands)
            received_response_count = 0

            while received_response_count < expected_responses:
                data = s.recv(4096).decode()
                if not data:
                    print(f"Warning: Server disconnected prematurely after {received_response_count}/{expected_responses} responses.")
                    break 
                
                recv_buffer += data
                
                while "\n" in recv_buffer and received_response_count < expected_responses:
                    response, recv_buffer = recv_buffer.split("\n", 1)
                    responses.append(response)
                    received_response_count += 1
                    
        except ConnectionRefusedError:
            print(f"Error: Connection to {HOST}:{PORT} refused. Is the server running?")
            responses.append("Error: Connection refused.")
        except Exception as e:
            print(f"Error during batch send: {e}")
            responses.append(f"Error: {e}")
    return responses