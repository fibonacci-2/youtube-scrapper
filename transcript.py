from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter
import sys

def get_video_id(url):
    """Extract the video ID from a YouTube URL."""
    if "v=" in url:
        return url.split("v=")[1].split("&")[0]
    elif "youtu.be/" in url:
        return url.split("youtu.be/")[1].split("?")[0]
    else:
        return url

def save_transcript(video_url, language='ar'):
    """Fetch and save the transcript of a YouTube video."""
    video_id = get_video_id(video_url)
    
    try:
        ytt_api = YouTubeTranscriptApi()
        
        # First, let's try to get the transcript using the older method that returns dicts
        try:
            # Try the legacy method first (returns list of dicts with 'text' keys)
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=[language])
            print(f"✅ Success using legacy method")
            
        except (AttributeError, TypeError):
            # Legacy method not available, try the new method
            print("Legacy method not available, trying v1.x method...")
            fetched = ytt_api.fetch(video_id, languages=[language])
            
            # Handle different return types
            if isinstance(fetched, list):
                # If it's already a list of dicts
                transcript_list = fetched
            elif hasattr(fetched, '__iter__'):
                # If it's iterable, check what it yields
                transcript_list = []
                for item in fetched:
                    if hasattr(item, 'text'):
                        # Object with text attribute
                        transcript_list.append({
                            'text': item.text,
                            'start': item.start,
                            'duration': item.duration
                        })
                    elif isinstance(item, dict) and 'text' in item:
                        # Dictionary with text key
                        transcript_list.append(item)
                    else:
                        print(f"Unknown item type: {type(item)}")
                        print(f"Item: {item}")
                        raise Exception("Unable to parse transcript items")
            else:
                raise Exception(f"Unknown return type: {type(fetched)}")
        
        # Now we have transcript_list as list of dicts with 'text' keys
        if not transcript_list:
            raise Exception("No transcript data retrieved")
        
        # Format and save
        formatter = TextFormatter()
        text_transcript = formatter.format_transcript(transcript_list)
        
        filename = f"{video_id}_transcript_{language}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(text_transcript)
        
        print(f"✅ Success! Transcript saved to {filename}")
        print(f"📊 Total entries: {len(transcript_list)}")
        print("\n--- First few lines of transcript ---")
        # Show first 5 entries or 300 chars
        preview = '\n'.join([entry['text'] for entry in transcript_list[:5]])
        print(preview[:300] + ("..." if len(preview) > 300 else ""))
        
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        print(f"\nTroubleshooting - Video ID: {video_id}")
        
        # Try to show available transcripts using different methods
        try:
            # Method 1
            available = ytt_api.list(video_id)
            print(f"\n📋 Available transcripts via .list():")
            for transcript in available:
                print(f"   - {transcript.language} ({transcript.language_code})")
        except:
            try:
                # Method 2
                from youtube_transcript_api._api import YouTubeTranscriptApi as YTApi
                yt = YTApi()
                transcript_list = yt.list_transcripts(video_id)
                print(f"\n📋 Available transcripts via .list_transcripts():")
                for transcript in transcript_list:
                    print(f"   - {transcript.language} ({transcript.language_code})")
            except Exception as e2:
                print(f"Could not list transcripts: {e2}")

# Use it
if __name__ == "__main__":
    video_url = "https://www.youtube.com/watch?v=CvrLRq1WPec"
    save_transcript(video_url, language='ar')