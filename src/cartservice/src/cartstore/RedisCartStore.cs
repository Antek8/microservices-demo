// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Caching.Distributed;
using Google.Protobuf;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Polly.Wrap;

namespace cartservice.cartstore
{
    public class RedisCartStore : ICartStore
    {
        private readonly IDistributedCache _cache;
        private readonly ConcurrentDictionary<string, Hipstershop.Cart> _fallbackCache;
        private readonly AsyncPolicyWrap _policyWrap;

        public RedisCartStore(IDistributedCache cache)
        {
            _cache = cache;
            _fallbackCache = new ConcurrentDictionary<string, Hipstershop.Cart>();
            
            var retryPolicy = Policy.Handle<Exception>()
                .WaitAndRetryAsync(
                    retryCount: 3,
                    sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        Console.WriteLine($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry.");
                    });

            var circuitBreakerPolicy = Policy.Handle<Exception>()
                .CircuitBreakerAsync(
                    exceptionsAllowedBeforeBreaking: 2,
                    durationOfBreak: TimeSpan.FromMinutes(1),
                    onBreak: (exception, duration) =>
                    {
                        Console.WriteLine($"Circuit breaker opened due to: {exception.Message}");
                    },
                    onReset: () => Console.WriteLine("Circuit breaker reset."),
                    onHalfOpen: () => Console.WriteLine("Circuit breaker half-open, next call is a trial.")
                );

            _policyWrap = Policy.WrapAsync(retryPolicy, circuitBreakerPolicy);
        }

        public async Task AddItemAsync(string userId, string productId, int quantity)
        {
            Console.WriteLine($"AddItemAsync called with userId={userId}, productId={productId}, quantity={quantity}");

            await _policyWrap.ExecuteAsync(async () =>
            {
                try
                {
                    Hipstershop.Cart cart;
                    var value = await _cache.GetAsync(userId);
                    if (value == null)
                    {
                        cart = new Hipstershop.Cart();
                        cart.UserId = userId;
                        cart.Items.Add(new Hipstershop.CartItem { ProductId = productId, Quantity = quantity });
                    }
                    else
                    {
                        cart = Hipstershop.Cart.Parser.ParseFrom(value);
                        var existingItem = cart.Items.SingleOrDefault(i => i.ProductId == productId);
                        if (existingItem == null)
                        {
                            cart.Items.Add(new Hipstershop.CartItem { ProductId = productId, Quantity = quantity });
                        }
                        else
                        {
                            existingItem.Quantity += quantity;
                        }
                    }
                    await _cache.SetAsync(userId, cart.ToByteArray());
                    _fallbackCache[userId] = cart; // Update fallback cache
                }
                catch (Exception ex)
                {
                    // Use fallback cache in case of failure
                    Console.WriteLine($"Failed to access Redis, falling back to in-memory cache: {ex.Message}");
                    if (!_fallbackCache.TryGetValue(userId, out var cart))
                    {
                        cart = new Hipstershop.Cart();
                        cart.UserId = userId;
                        cart.Items.Add(new Hipstershop.CartItem { ProductId = productId, Quantity = quantity });
                        _fallbackCache[userId] = cart;
                    }
                    else
                    {
                        var existingItem = cart.Items.SingleOrDefault(i => i.ProductId == productId);
                        if (existingItem == null)
                        {
                            cart.Items.Add(new Hipstershop.CartItem { ProductId = productId, Quantity = quantity });
                        }
                        else
                        {
                            existingItem.Quantity += quantity;
                        }
                    }
                }
            });
        }

        public async Task EmptyCartAsync(string userId)
        {
            Console.WriteLine($"EmptyCartAsync called with userId={userId}");

            await _policyWrap.ExecuteAsync(async () =>
            {
                try
                {
                    var cart = new Hipstershop.Cart();
                    await _cache.SetAsync(userId, cart.ToByteArray());
                    _fallbackCache[userId] = cart; // Update fallback cache
                }
                catch (Exception ex)
                {
                    // Use fallback cache in case of failure
                    Console.WriteLine($"Failed to access Redis, falling back to in-memory cache: {ex.Message}");
                    _fallbackCache[userId] = new Hipstershop.Cart();
                }
            });
        }

        public async Task<Hipstershop.Cart> GetCartAsync(string userId)
        {
            Console.WriteLine($"GetCartAsync called with userId={userId}");

            return await _policyWrap.ExecuteAsync(async () =>
            {
                try
                {
                    // Access the cart from the cache
                    var value = await _cache.GetAsync(userId);

                    if (value != null)
                    {
                        var cart = Hipstershop.Cart.Parser.ParseFrom(value);
                        _fallbackCache[userId] = cart; // Update fallback cache
                        return cart;
                    }

                    // We decided to return empty cart in cases when user wasn't in the cache before
                    return new Hipstershop.Cart();
                }
                catch (Exception ex)
                {
                    // Use fallback cache in case of failure
                    Console.WriteLine($"Failed to access Redis, falling back to in-memory cache: {ex.Message}");
                    if (_fallbackCache.TryGetValue(userId, out var cart))
                    {
                        return cart;
                    }

                    // Return an empty cart if the fallback cache doesn't have the entry
                    return new Hipstershop.Cart();
                }
            });
        }

        public bool Ping()
        {
            try
            {
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}