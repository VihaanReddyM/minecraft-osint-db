import requests
import time
from collections import deque

def build_friend_mesh(start_uuid, max_depth=2, delay_seconds=1.0):
    """
    Builds a mesh of friends from the NameMC API.
    
    :param start_uuid: The initial player UUID to start from.
    :param max_depth: How many levels deep to go (1 = friends, 2 = friends of friends).
    :param delay_seconds: Time to wait between API calls to avoid looking suspicious.
    """
    
    base_url = "https://api.namemc.com/profile/{}/friends"
    
    # Track which UUIDs we have already sent an API call for
    visited_uuids = set()
    
    # The final mesh (graph) showing who is friends with who
    # Format: { "UUID_A": ["UUID_B", "UUID_C"], ... }
    mesh = {}
    
    # Queue for BFS traversal: stores tuples of (uuid, current_depth)
    queue = deque([(start_uuid, 0)])
    
    print(f"Starting mesh build for {start_uuid} (Max Depth: {max_depth})")

    while queue:
        current_uuid, current_depth = queue.popleft()

        # Stop if we've reached the maximum depth we want to scrape
        if current_depth > max_depth:
            continue

        # Skip if we already queried this UUID (prevents duplicate calls)
        if current_uuid in visited_uuids:
            continue
            
        print(f"Fetching friends for: {current_uuid} (Depth {current_depth})")
        
        try:
            # Make the API call
            response = requests.get(base_url.format(current_uuid))
            
            # Handle rate limits or errors
            if response.status_code == 429:
                print("Rate limited! Waiting 10 seconds...")
                time.sleep(10)
                # Put the UUID back in the queue to try again
                queue.appendleft((current_uuid, current_depth))
                continue
            elif response.status_code != 200:
                print(f"Failed to fetch {current_uuid}: Status {response.status_code}")
                visited_uuids.add(current_uuid) # Mark as visited so we don't get stuck
                continue

            friends_data = response.json()
            
            # Extract just the UUIDs from the friend list
            friend_uuids = [friend["uuid"] for friend in friends_data]
            
            # Add to our mesh dictionary
            mesh[current_uuid] = friend_uuids
            
            # Add this UUID to visited so we NEVER query it again
            visited_uuids.add(current_uuid)
            
            # Add all the fetched friends to the queue to be processed next
            for f_uuid in friend_uuids:
                if f_uuid not in visited_uuids:
                    queue.append((f_uuid, current_depth + 1))
                    
        except Exception as e:
            print(f"Error fetching {current_uuid}: {e}")
            
        # The most important part: don't spam the API!
        time.sleep(delay_seconds)

    print("\nFinished building the mesh!")
    return mesh

# --- How to use it ---
if __name__ == "__main__":
    # Replace with your starting UUID
    INITIAL_UUID = "57a0be0c-30fa-4ab0-aa2b-40091dbdf7e0" 
    
    # I highly recommend keeping depth to 2 to start. 
    # If a user has 50 friends, and they each have 50 friends, that's 2,500 API calls!
    friend_graph = build_friend_mesh(INITIAL_UUID, max_depth=2, delay_seconds=1.5)
    
    # Print the resulting mesh (or you could save it to a JSON file)
    import json
    with open("friend_mesh.json", "w") as f:
        json.dump(friend_graph, f, indent=4)
        
    print("Mesh saved to friend_mesh.json")