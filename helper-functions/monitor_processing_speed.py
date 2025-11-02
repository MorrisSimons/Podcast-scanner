#!/usr/bin/env python3
import subprocess
import time
import json
from datetime import datetime

def get_queue_length():
    """Get current queue length from Redis"""
    cmd = [
        "redis-cli", "-h", "163.172.143.150", "-p", "6379",
        "--user", "morris-redis", "-a", "RMC-gxa1wnw8zwc5uax",
        "--tls", "--cacert", "/Users/morrissimons/Desktop/Podcast scanner/SSL_redis-redis-epic-wing.pem",
        "XLEN", "podcast:queue"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    # Extract the integer from output (skip warning line)
    for line in result.stdout.split('\n'):
        if line and not line.startswith('Warning'):
            try:
                return int(line)
            except:
                pass
    return 0

def get_pending_count():
    """Get pending message count from consumer group"""
    cmd = [
        "redis-cli", "-h", "163.172.143.150", "-p", "6379",
        "--user", "morris-redis", "-a", "RMC-gxa1wnw8zwc5uax",
        "--tls", "--cacert", "/Users/morrissimons/Desktop/Podcast scanner/SSL_redis-redis-epic-wing.pem",
        "XINFO", "GROUPS", "podcast:queue"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    lines = result.stdout.split('\n')
    for i, line in enumerate(lines):
        if '"pending"' in line and i+1 < len(lines):
            try:
                next_line = lines[i+1].strip()
                # Extract integer from format like "6) (integer) 10023"
                if '(integer)' in next_line:
                    return int(next_line.split('(integer)')[-1].strip())
                else:
                    return int(next_line)
            except:
                pass
    return 0

def get_processed_count():
    """Get total processed message count from Redis counter"""
    cmd = [
        "redis-cli", "-h", "163.172.143.150", "-p", "6379",
        "--user", "morris-redis", "-a", "RMC-gxa1wnw8zwc5uax",
        "--tls", "--cacert", "/Users/morrissimons/Desktop/Podcast scanner/SSL_redis-redis-epic-wing.pem",
        "GET", "podcast:processed_count"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    for line in result.stdout.split('\n'):
        if line and not line.startswith('Warning'):
            try:
                return int(line.strip('"'))
            except:
                pass
    return 0

def main():
    print("Starting Redis queue processing speed monitor...")
    print("Collecting 50 samples (5 seconds apart)...")
    print("-" * 70)
    
    samples = []
    start_time = time.time()
    
    # Get initial readings
    initial_length = get_queue_length()
    initial_pending = get_pending_count()
    initial_processed = get_processed_count()
    previous_processed = initial_processed
    
    print(f"Initial queue length: {initial_length:,}")
    print(f"Initial pending: {initial_pending:,}")
    print(f"Initial processed count: {initial_processed:,}")
    print("-" * 70)
    
    for i in range(50):
        # Wait 5 seconds between samples (except for first sample)
        if i > 0:
            time.sleep(5)
        
        current_length = get_queue_length()
        pending = get_pending_count()
        current_processed = get_processed_count()
        current_time = time.time()
        elapsed = current_time - start_time
        
        # Calculate actual items processed in this interval
        items_processed_this_interval = current_processed - previous_processed
        
        samples.append({
            'time': elapsed,
            'queue_length': current_length,
            'pending': pending,
            'timestamp': datetime.now().isoformat()
        })
        
        # Calculate cumulative statistics
        total_processed = current_processed - initial_processed
        previous_processed = current_processed
        
        if elapsed > 0:
            # Overall rates since start
            overall_rate_per_second = total_processed / elapsed if total_processed > 0 else 0
            overall_rate_per_minute = overall_rate_per_second * 60
            overall_rate_per_hour = overall_rate_per_minute * 60
            
            # Estimate time remaining based on overall rate
            if overall_rate_per_second > 0:
                time_remaining_seconds = current_length / overall_rate_per_second
                hours = int(time_remaining_seconds // 3600)
                minutes = int((time_remaining_seconds % 3600) // 60)
                seconds = int(time_remaining_seconds % 60)
                eta = f"{hours}h {minutes}m {seconds}s"
            else:
                eta = "N/A"
            
            # Print progress every 10 samples
            if (i + 1) % 10 == 0 or i == 0:
                print(f"Sample {i+1}/50:")
                print(f"  Current queue: {current_length:,} | Pending: {pending:,}")
                print(f"  Total processed: {total_processed:,} items in {elapsed:.1f}s")
                print(f"  Last interval: {items_processed_this_interval:,} items in 5s")
                print(f"  Overall rate: {overall_rate_per_second:.2f}/sec | {overall_rate_per_minute:.1f}/min | {overall_rate_per_hour:.0f}/hour")
                print(f"  ETA: {eta}")
                print("-" * 70)
    
    # Final analysis
    total_elapsed = time.time() - start_time
    final_length = samples[-1]['queue_length']
    final_processed = get_processed_count()
    total_processed = final_processed - initial_processed
    
    # Calculate average rate
    avg_rate_per_second = total_processed / total_elapsed if total_elapsed > 0 else 0
    avg_rate_per_minute = avg_rate_per_second * 60
    avg_rate_per_hour = avg_rate_per_minute * 60
    
    print("\n" + "=" * 70)
    print("FINAL ANALYSIS (50 samples over ~4.2 minutes):")
    print(f"  Total time: {total_elapsed:.1f} seconds")
    print(f"  Initial queue: {initial_length:,}")
    print(f"  Final queue: {final_length:,}")
    print(f"  Queue change: {final_length - initial_length:+,} items")
    print(f"  Total processed: {total_processed:,} items")
    print("\nOverall Processing Rates:")
    print(f"  Average: {avg_rate_per_second:.2f} items/second")
    print(f"  Average: {avg_rate_per_minute:.1f} items/minute")
    print(f"  Average: {avg_rate_per_hour:.0f} items/hour")
    print(f"\nWith 7 GPUs:")
    print(f"  Per GPU: {avg_rate_per_second/7:.2f} items/second/GPU")
    
    # Save detailed results
    output_path = '/Users/morrissimons/Desktop/Podcast scanner/performance-gains-log/processing_speed_results.json'
    with open(output_path, 'w') as f:
        json.dump({
            'samples': samples,
            'summary': {
                'total_time_seconds': total_elapsed,
                'total_processed': total_processed,
                'avg_rate_per_second': avg_rate_per_second,
                'avg_rate_per_minute': avg_rate_per_minute,
                'avg_rate_per_hour': avg_rate_per_hour,
                'per_gpu_rate': avg_rate_per_second / 7
            }
        }, f, indent=2)
    print(f"\nDetailed results saved to {output_path}")

if __name__ == "__main__":
    main()