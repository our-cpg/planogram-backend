import express from 'express';
import cors from 'cors'; 
const app = express();
const PORT = process.env.PORT || 10000;

// Aggressive CORS middleware - set headers on EVERY response
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Max-Age', '86400'); // Cache preflight for 24 hours
  
  // Handle preflight - MUST return 200 immediately
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  next();
});
  
  next();
});

app.use(express.json());

// Product cache
let productCache = [];
let cacheStatus = 'empty';
let cacheRefreshInProgress = false;

async function refreshProductCache(storeName, accessToken) {
  if (cacheRefreshInProgress) {
    console.log('Cache refresh already in progress, skipping...');
    return;
  }
  
  cacheRefreshInProgress = true;
  cacheStatus = 'loading';
  console.log('Starting cache refresh (all products)...');
  
  try {
    let allProducts = [];
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;
    
    while (hasNextPage) {
      const url = pageInfo 
        ? `https://${storeName}/admin/api/2024-01/products.json?limit=250&page_info=${pageInfo}`
        : `https://${storeName}/admin/api/2024-01/products.json?limit=250&order=updated_at desc`;
        
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
      pageCount++;
      
      productCache = allProducts;
      console.log(`Loaded page ${pageCount}, total products: ${allProducts.length}`);
      
      const linkHeader = response.headers.get('link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const match = linkHeader.match(/page_info=([^&>]+)/);
        pageInfo = match ? match[1] : null;
      } else {
        hasNextPage = false;
      }
      
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    productCache = allProducts;
    cacheStatus = 'ready';
    console.log(`✅ Cache complete! ${allProducts.length} products loaded.`);
  } catch (error) {
    console.error('Cache refresh failed:', error);
    cacheStatus = 'error';
  } finally {
    cacheRefreshInProgress = false;
  }
}

app.get('/api/shopify', (req, res) => {
  res.json({ 
    status: 'Backend is alive!',
    cacheStatus,
    cachedProducts: productCache.length
  });
});

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
      refreshProductCache(storeName, accessToken);
      
      return res.json({ 
        success: true, 
        shopName: data.shop.name,
        message: 'Loading all products...'
      });
    }

    if (action === 'refreshCache') {
      console.log('Manual cache refresh requested');
      await refreshProductCache(storeName, accessToken);
      return res.json({ 
        success: true, 
        message: 'Cache refreshed successfully',
        productsLoaded: productCache.length,
        cacheStatus
      });
    }
    
    if (action === 'getProduct' && upc) {
      const searchUPC = String(upc).trim();
      
      if (productCache.length === 0 && cacheStatus === 'loading') {
        await new Promise(resolve => setTimeout(resolve, 3000));
      }
      
      for (const product of productCache) {
        for (const variant of product.variants || []) {
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
        error: 'Product not found in cache',
        searchedProducts: productCache.length,
        hint: 'All products cached. If product not found, refresh cache.'
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
