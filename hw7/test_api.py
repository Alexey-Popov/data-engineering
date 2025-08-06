#!/usr/bin/env python3
"""
Test script for Amazon Reviews API
Tests all endpoints and validates responses
"""

import requests
import json
import time
from datetime import datetime

BASE_URL = "http://localhost:8000"

def test_health():
    """Test health endpoint"""
    print("Testing health endpoint...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        if response.status_code == 200:
            print("✅ Health check passed")
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Health check error: {e}")
        return False

def test_product_reviews():
    """Test product reviews endpoint"""
    print("\nTesting product reviews endpoint...")
    try:
        # Test with a sample product ID
        product_id = "0439784549"
        response = requests.get(f"{BASE_URL}/reviews/product/{product_id}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Product reviews test passed")
            print(f"   Found {data.get('total_count', 0)} reviews for product {product_id}")
            return True
        else:
            print(f"❌ Product reviews test failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Product reviews test error: {e}")
        return False

def test_product_reviews_by_rating():
    """Test product reviews by rating endpoint"""
    print("\nTesting product reviews by rating endpoint...")
    try:
        product_id = "0439784549"
        star_rating = 5
        response = requests.get(f"{BASE_URL}/reviews/product/{product_id}/rating/{star_rating}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Product reviews by rating test passed")
            print(f"   Found {data.get('total_count', 0)} {star_rating}-star reviews for product {product_id}")
            return True
        else:
            print(f"❌ Product reviews by rating test failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Product reviews by rating test error: {e}")
        return False

def test_customer_reviews():
    """Test customer reviews endpoint"""
    print("\nTesting customer reviews endpoint...")
    try:
        # Test with a sample customer ID
        customer_id = "A2ZR3CQJ5V5QKX"
        response = requests.get(f"{BASE_URL}/reviews/customer/{customer_id}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Customer reviews test passed")
            print(f"   Found {data.get('total_count', 0)} reviews for customer {customer_id}")
            return True
        else:
            print(f"❌ Customer reviews test failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Customer reviews test error: {e}")
        return False

def test_analytics_endpoints():
    """Test analytics endpoints"""
    print("\nTesting analytics endpoints...")
    
    endpoints = [
        ("most-reviewed-items", "Most reviewed items"),
        ("most-productive-customers", "Most productive customers"),
        ("most-productive-haters", "Most productive haters"),
        ("most-productive-backers", "Most productive backers")
    ]
    
    success_count = 0
    
    for endpoint, name in endpoints:
        try:
            response = requests.get(f"{BASE_URL}/analytics/{endpoint}?period=2015-01&limit=5")
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ {name} test passed")
                print(f"   Found {data.get('total_count', 0)} results for period 2015-01")
                success_count += 1
            else:
                print(f"❌ {name} test failed: {response.status_code}")
                
        except Exception as e:
            print(f"❌ {name} test error: {e}")
    
    return success_count == len(endpoints)

def test_cache_performance():
    """Test cache performance by making repeated requests"""
    print("\nTesting cache performance...")
    
    try:
        # First request (should hit database)
        start_time = time.time()
        response1 = requests.get(f"{BASE_URL}/reviews/product/0439784549")
        first_request_time = time.time() - start_time
        
        # Second request (should hit cache)
        start_time = time.time()
        response2 = requests.get(f"{BASE_URL}/reviews/product/0439784549")
        second_request_time = time.time() - start_time
        
        if response1.status_code == 200 and response2.status_code == 200:
            print(f"✅ Cache performance test passed")
            print(f"   First request (DB): {first_request_time:.3f}s")
            print(f"   Second request (Cache): {second_request_time:.3f}s")
            print(f"   Speedup: {first_request_time/second_request_time:.1f}x")
            return True
        else:
            print(f"❌ Cache performance test failed")
            return False
            
    except Exception as e:
        print(f"❌ Cache performance test error: {e}")
        return False

def main():
    """Run all tests"""
    print("=== Amazon Reviews API Test Suite ===")
    print(f"Testing API at: {BASE_URL}")
    print(f"Timestamp: {datetime.now()}")
    
    tests = [
        ("Health Check", test_health),
        ("Product Reviews", test_product_reviews),
        ("Product Reviews by Rating", test_product_reviews_by_rating),
        ("Customer Reviews", test_customer_reviews),
        ("Analytics Endpoints", test_analytics_endpoints),
        ("Cache Performance", test_cache_performance)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
    
    print(f"\n=== Test Results ===")
    print(f"Passed: {passed}/{total}")
    print(f"Success Rate: {(passed/total)*100:.1f}%")
    
    if passed == total:
        print("🎉 All tests passed! API is working correctly.")
    else:
        print("⚠️  Some tests failed. Check the logs above for details.")
    
    return passed == total

if __name__ == "__main__":
    main() 