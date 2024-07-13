#!/usr/bin/env python

import asyncio
from chessdotcom.aio import get_player_profile

usernames = ["fabianocaruana", "GMHikaruOnTwitch", "MagnusCarlsen", "GarryKasparov"]

async def fetch_profiles(usernames):
    # Create a list of coroutines (async tasks)
    cors = [get_player_profile(name) for name in usernames]

    # Gather all the coroutines concurrently
    responses = await asyncio.gather(*cors)

    return responses

async def main():
    responses = await fetch_profiles(usernames)
    
    for username, response in zip(usernames, responses):
        print(f"Profile for {username}:")
        print(response)
        print()  # Just for separating each profile visually

# Run the asyncio event loop with the main coroutine
if __name__ == "__main__":
    asyncio.run(main())