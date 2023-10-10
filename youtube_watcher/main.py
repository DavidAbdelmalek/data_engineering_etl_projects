import logging
import sys
from typing import Optional
import requests
from datetime import datetime
from config import config
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer

import requests
import logging
from typing import Optional, Dict


def fetch_playlist_item_page(key: str, playlistId: str, page_token: Optional[str] = None) -> Dict:
    """
    Fetches a page of playlist items from the YouTube Data API.

    :param key: The API key to authenticate the request.
    :param playlistId: The ID of the playlist from which to fetch the items.
    :param page_token: (Optional) The token for the next page of results.
    :return: The JSON payload containing the fetched playlist items.
    """
    logging.info("Starting")
    res = requests.get(
        "https://youtube.googleapis.com/youtube/v3/playlistItems",
        params={"key": key,
                "playlistId": playlistId,
                "part": "contentDetails",
                "pageToken": page_token
                }
    )

    payload = res.json()
    return payload


def fetch_videos_page(key, video_id, page_token=None):
    """
    Fetches a page of videos from the YouTube API.

    Parameters:
    - key (str): The API key to authenticate the request.
    - video_id (str): The ID of the video to fetch.
    - page_token (str, optional): The token for the next page of results (default: None).

    Returns:
    - dict: The JSON response containing the video information.
    """
    logging.info("Starting")
    res = requests.get("https://youtube.googleapis.com/youtube/v3/videos", params={"key": key, "id": video_id, "part": "snippet,statistics", "pageToken": page_token
                                                                                   })

    payload = res.json()
    return payload


def fetch_playlist_items(key, playlistId, page_token=None):
    """
    Fetches playlist items from the API.

    Args:
        key (str): API key.
        playlistId (str): ID of the playlist.
        page_token (str, optional): Page token for pagination. Defaults to None.

    Yields:
        dict: Payload containing playlist items.

    """
    logging.info("Start Fetching playlist items")
    # Fetch first page
    payload = fetch_playlist_item_page(key, playlistId, page_token)
    playlistIems = payload['items']

    if 'nextPageToken' in payload.keys():
        # Fetch next page recursively
        next_page_token = payload['nextPageToken']
        playlistIems += fetch_playlist_items(key, playlistId, next_page_token)

    return playlistIems


def fetch_videos(key: str, videoId: str, page_token: Optional[str] = None):
    """
    Fetches playlist items from the API.

    Args:
        key: API key.
        videoId: ID of the playlist.
        page_token: Page token for pagination. Defaults to None.

    Yields:
        Payload containing playlist items.

    """
    logging.info("Start Fetching playlist items")
    # Fetch first page
    payload = fetch_videos_page(key, videoId, page_token)
    video_item = payload['items']

    if 'nextPageToken' in payload.keys():
        # Fetch next page recursively
        next_page_token = payload['nextPageToken']
        video_item += fetch_videos(key, videoId, next_page_token)

    return video_item


def summarize_video(video):
    return {
        "TITLE": video['snippet']['title'],
        "VIEWS": int(video['statistics'].get('viewCount', 0)),
        "COMMENTS": int(video['statistics'].get('commentCount', 0)),
        "LIKES": int(video['statistics'].get('likeCount', 0)),
        "DISLIKES": int(video['statistics'].get('dislikeCount', 0)),
        "PUBLISHED_AT": str(datetime.strptime(video['snippet']['publishedAt'],
                                              '%Y-%m-%dT%H:%M:%SZ').date())
    }


def on_delivery(err, msg):
    if err is not None:
        logging.error("Delivery failed: {}".format(err))
    else:
        logging.info("Delivered message: {}".format(msg))


def main():
    """
        Retrieves the schema for the "youtube_videos-value" from the Schema Registry.
        Configures the Kafka producer with the necessary serializers.
        Fetches the playlist items from the YouTube API using the provided API key and playlist ID.
        Fetches the video details for each video in the playlist.
        Summarizes each video and produces it to the "youtube_videos" topic in Kafka.
    """
    logging.info("Starting")

    logging.info("Get Schema from KAFKA")
    schema_registry_client = SchemaRegistryClient(config["schema_registry"])
    youtube_videos_schema = schema_registry_client.get_latest_version(
        "youtube_videos-value")

    kafka_config = config["kafka"] | {
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(
            schema_registry_client,
            youtube_videos_schema.schema.schema_str)
    }
    producer = SerializingProducer(kafka_config)

    key = config["youtube_api_key"]
    playlistId = config["youtube_playlist_id"]
    logging.info("Fetching playlist items")
    playlist_items = fetch_playlist_items(key=key, playlistId=playlistId)

    for video_item in playlist_items:
        video_id = video_item['contentDetails']['videoId']
        logging.info("Fetching video id {}".format(video_id))
        video_items = fetch_videos(key=key, videoId=video_id)
        for video in video_items:
            item = summarize_video(video)
            producer.produce(
                topic="youtube_videos",
                key=video_id,
                value=item,
                on_delivery=on_delivery
            )

    producer.flush()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    sys.exit(main())
