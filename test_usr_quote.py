#!/usr/bin/env python3

import requests
import json
from decimal import Decimal

def test_usr_quote():
    """Test direct CoWSwap API call for USR -> USDC on Base"""
    
    # Exact same values from the logs
    sell_token = "0x35E5dB674D8e93a03d814FA0ADa70731efe8a4b9"  # USR on Base
    buy_token = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"   # USDC on Base
    amount = "5865500868442103551237"  # 5865.5 USR in wei
    decimals = 18
    
    print("="*80)
    print("DIRECT COWSWAP API TEST - USR -> USDC on Base")
    print("="*80)
    
    print(f"\nRequest Parameters:")
    print(f"Sell Token (USR): {sell_token}")
    print(f"Buy Token (USDC): {buy_token}")
    print(f"Amount: {amount} wei")
    print(f"Amount Normalized: {Decimal(amount) / Decimal(10**decimals)} USR")
    print(f"Decimals: {decimals}")
    
    # CoWSwap API endpoint for Base
    url = "https://api.cow.fi/base/api/v1/quote"
    
    # Request payload (exactly like cow_client.py)
    from datetime import datetime, timezone
    
    payload = {
        "sellToken": sell_token,
        "buyToken": buy_token,
        "sellAmountBeforeFee": amount,  # Key difference!
        "from": "0x0000000000000000000000000000000000000000",  # Zero address
        "receiver": "0x0000000000000000000000000000000000000000",  # Zero address
        "validTo": int(datetime.now(timezone.utc).timestamp() + 3600),  # 1 hour validity
        "appData": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "partiallyFillable": False,
        "sellTokenBalance": "erc20",
        "buyTokenBalance": "erc20",
        "kind": "sell",
        "priceQuality": "fast"
    }
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    print(f"\nAPI Endpoint: {url}")
    print(f"Headers: {json.dumps(headers, indent=2)}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    try:
        print(f"\nMaking request...")
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        print(f"Response Status: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\nResponse Body:")
            print(json.dumps(data, indent=2))
            
            # Calculate the rate
            buy_amount = int(data["quote"]["buyAmount"])
            sell_amount = int(amount)
            
            # Convert to normalized amounts
            usdc_normalized = Decimal(buy_amount) / Decimal(10**6)  # USDC has 6 decimals
            usr_normalized = Decimal(sell_amount) / Decimal(10**18)  # USR has 18 decimals
            
            rate = usdc_normalized / usr_normalized
            
            print(f"\nCalculated Results:")
            print(f"Sell Amount: {usr_normalized} USR")
            print(f"Buy Amount: {usdc_normalized} USDC") 
            print(f"Rate: {rate:.6f} USDC/USR")
            
            # Check if this rate makes sense
            if rate < 0.5:
                print(f"\n❌ RATE ALERT: {rate:.6f} USDC/USR is very low for a stablecoin!")
                print("This suggests either:")
                print("1. USR is severely depegged on Base")
                print("2. Very low liquidity for USR on Base")
                print("3. Wrong token address")
                print("4. CoWSwap doesn't have good routing for USR on Base")
            else:
                print(f"\n✅ Rate looks reasonable: {rate:.6f} USDC/USR")
                
        else:
            print(f"\nError Response:")
            print(response.text)
            
    except Exception as e:
        print(f"\nError making request: {str(e)}")

def test_direct_rate_check():
    """Additional test to check USR rate vs other stablecoins"""
    from datetime import datetime, timezone
    
    print(f"\n" + "="*80)
    print("ADDITIONAL RATE COMPARISON")
    print("="*80)
    
    # Test with 1 USR (1e18 wei)
    test_amount = "1000000000000000000"  # 1 USR exactly
    
    print(f"\nTesting with 1 USR ({test_amount} wei)")
    
    payload = {
        "sellToken": "0x35E5dB674D8e93a03d814FA0ADa70731efe8a4b9",  # USR
        "buyToken": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",   # USDC
        "sellAmountBeforeFee": test_amount,  # Key difference!
        "from": "0x0000000000000000000000000000000000000000",  # Zero address
        "receiver": "0x0000000000000000000000000000000000000000",  # Zero address
        "validTo": int(datetime.now(timezone.utc).timestamp() + 3600),
        "appData": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "partiallyFillable": False,
        "sellTokenBalance": "erc20",
        "buyTokenBalance": "erc20",
        "kind": "sell",
        "priceQuality": "fast"
    }
    
    try:
        response = requests.post(
            "https://api.cow.fi/base/api/v1/quote",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            buy_amount = int(data["quote"]["buyAmount"])
            usdc_amount = Decimal(buy_amount) / Decimal(10**6)
            
            print(f"1 USR = {usdc_amount} USDC")
            
            if usdc_amount < 0.5:
                print("❌ Confirmed: USR rate is abnormally low on Base")
            else:
                print("✅ USR rate looks normal")
        else:
            print(f"Error: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"Error: {str(e)}")

def test_consecutive_requests():
    """Test two consecutive requests with the same large amount to check for caching/timing issues"""
    from datetime import datetime, timezone
    import time
    
    print(f"\n" + "="*80)
    print("CONSECUTIVE REQUESTS TEST - Same Amount Twice")
    print("="*80)
    
    # Same large amount as before
    sell_token = "0x35E5dB674D8e93a03d814FA0ADa70731efe8a4b9"  # USR on Base
    buy_token = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"   # USDC on Base
    amount = "5865500868442103551237"  # 5865.5 USR in wei
    
    print(f"Testing with {Decimal(amount) / Decimal(10**18)} USR")
    print(f"Amount: {amount} wei")
    
    # URL and headers
    url = "https://api.cow.fi/base/api/v1/quote"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    def make_single_request(request_num):
        print(f"\n--- Request #{request_num} ---")
        
        payload = {
            "sellToken": sell_token,
            "buyToken": buy_token,
            "sellAmountBeforeFee": amount,
            "from": "0x0000000000000000000000000000000000000000",
            "receiver": "0x0000000000000000000000000000000000000000",
            "validTo": int(datetime.now(timezone.utc).timestamp() + 3600),
            "appData": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "partiallyFillable": False,
            "sellTokenBalance": "erc20",
            "buyTokenBalance": "erc20",
            "kind": "sell",
            "priceQuality": "fast"
        }
        
        try:
            print(f"Making request {request_num}...")
            start_time = time.time()
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            end_time = time.time()
            
            print(f"Response time: {end_time - start_time:.2f}s")
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                buy_amount = int(data["quote"]["buyAmount"])
                sell_amount_actual = int(data["quote"]["sellAmount"])
                fee_amount = int(data["quote"]["feeAmount"])
                
                # Calculate rates
                usdc_normalized = Decimal(buy_amount) / Decimal(10**6)
                usr_normalized = Decimal(sell_amount_actual) / Decimal(10**18)
                rate = usdc_normalized / usr_normalized
                
                # Calculate fee percentage
                fee_percentage = (Decimal(fee_amount) / Decimal(amount)) * 100
                
                print(f"Buy Amount: {usdc_normalized} USDC")
                print(f"Sell Amount (actual): {usr_normalized} USR")
                print(f"Fee Amount: {Decimal(fee_amount) / Decimal(10**18)} USR")
                print(f"Fee Percentage: {fee_percentage:.4f}%")
                print(f"Rate: {rate:.6f} USDC/USR")
                
                return {
                    "success": True,
                    "buy_amount": buy_amount,
                    "sell_amount": sell_amount_actual,
                    "fee_amount": fee_amount,
                    "rate": rate,
                    "response_time": end_time - start_time
                }
            else:
                print(f"Error: {response.text}")
                return {"success": False, "error": response.text}
                
        except Exception as e:
            print(f"Exception: {str(e)}")
            return {"success": False, "error": str(e)}
    
    # Make first request
    result1 = make_single_request(1)
    
    # Wait a bit between requests
    print(f"\nWaiting 2 seconds...")
    time.sleep(2)
    
    # Make second request
    result2 = make_single_request(2)
    
    # Compare results
    print(f"\n" + "="*50)
    print("COMPARISON")
    print("="*50)
    
    if result1["success"] and result2["success"]:
        print(f"Request 1 - Rate: {result1['rate']:.6f} USDC/USR, Time: {result1['response_time']:.2f}s")
        print(f"Request 2 - Rate: {result2['rate']:.6f} USDC/USR, Time: {result2['response_time']:.2f}s")
        
        rate_diff = abs(result1['rate'] - result2['rate'])
        rate_diff_pct = (rate_diff / result1['rate']) * 100
        
        print(f"Rate difference: {rate_diff:.6f} ({rate_diff_pct:.2f}%)")
        
        if rate_diff_pct > 1:
            print("❌ Significant rate difference detected!")
        else:
            print("✅ Rates are consistent")
    else:
        print("❌ One or both requests failed")

if __name__ == "__main__":
    test_usr_quote()
    test_direct_rate_check()
    test_consecutive_requests() 