import express from 'express';
import cors from 'cors';
const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(cors());
app.use(express.json());

// Product cache
let productCache = null;
let cacheTimestamp = null;
const CACHE_DURATION = 60 * 60 * 1000; // 1 hour

// Function to fetch and cache all products
async function refreshProductCache(storeName, accessToken) {
  console.log('Refreshing product cache...');
  let allProducts = [];
  let hasNextPage = true;
  let pageInfo = null;
  
  while (hasNextPage) {
    const url = pageInfo 
      ? `https://${storeName}/admin/api/2024-01/products.json?limit=250&page_info=${pageInfo}`
      : `https://${storeName}/admin/api/2024-01/products.json?limit=250`;
      
    const response = await fetch(url, {
      headers: {
        'X-Shopify-Access-Token': accessToken,
        'Content-Type': 'application/json'
      }
    });
    
    if (!response.ok) {
      throw new Error('Failed to fetch products');
    }
    
    const data = await response.json();
    allProducts = allProducts.concat(data.products);
    
    const linkHeader = response.headers.get('link');
    if (linkHeader && linkHeader.includes('rel="next"')) {
      const match = linkHeader.match(/page_info=([^&>]+)/);
      pageInfo = match ? match[1] : null;
    } else {
      hasNextPage = false;
    }
  }
  
  productCache = allProducts;
  cacheTimestamp = Date.now();
  console.log(`Cache refreshed! ${allProducts.length} products loaded.`);
  return allProducts;
}

// Test endpoint
app.get('/api/shopify', (req, res) => {
  res.json({ 
    status: 'Backend is alive!',
    cachedProducts: productCache ? productCache.length : 0,
    cacheAge: cacheTimestamp ? Math.floor((Date.now() - cacheTimestamp) / 1000) : null
  });
});

// Shopify endpoints
app.post('/api/shopify', async (req, res) => {
  const { storeName, accessToken, action, upc } = req.body || {};
  
  try {
    if (action === 'test') {
      const response = await fetch(`https://${storeName}/admin/api/2024-01/shop.json`, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });
      
      if (!response.ok) {
        return res.status(response.status).json({ error: 'Shopify API error' });
      }
      
      const data = await response.json();
      
      // Refresh cache on connect
      refreshProductCache(storeName, accessToken).catch(err => {
        console.error('Failed to refresh cache:', err);
      });
      
      return res.json({ success: true, shopName: data.shop.name });
    }
    
    if (action === 'getProduct' && upc) {
      const searchUPC = String(upc).trim();
      
      // Check if cache is stale or empty
      if (!productCache || !cacheTimestamp || (Date.now() - cacheTimestamp) > CACHE_DURATION) {
        // Refresh cache in background, but still search through what we have
        if (productCache) {
          refreshProductCache(storeName, accessToken).catch(err => {
            console.error('Background cache refresh failed:', err);
          });
        } else {
          // No cache at all, need to fetch now
          await refreshProductCache(storeName, accessToken);
        }
      }
      
      // Search through cached products
      for (const product of productCache || []) {
        for (const variant of product.variants) {
          if (String(variant.barcode || '').trim() === searchUPC) {
            return res.json({
              success: true,
              product: {
                name: `${product.title}${variant.title !== 'Default Title' ? ' - ' + variant.title : ''}`,
                price: parseFloat(variant.price),
                cost: parseFloat(variant.compare_at_price || variant.price * 0.5),
                monthlySales: Math.floor(Math.random() * 200) + 50,
                sku: variant.sku
              }
            });
          }
        }
      }
      
      return res.status(404).json({ 
        error: 'Product not found',
        searchedProducts: productCache ? productCache.length : 0,
        searchingFor: searchUPC
      });
    }
    
    return res.json({ message: 'Backend ready' });
    
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
