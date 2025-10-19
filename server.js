import express from 'express';
import cors from 'cors';
const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(cors());
app.use(express.json());

// Product cache
let productCache = [];
let cacheStatus = 'empty'; // empty, loading, ready
let totalProductsExpected = 0;

// Background cache refresh
let cacheRefreshInProgress = false;

async function refreshProductCache(storeName, accessToken) {
  if (cacheRefreshInProgress) {
    console.log('Cache refresh already in progress, skipping...');
    return;
  }
  
  cacheRefreshInProgress = true;
  cacheStatus = 'loading';
  console.log('Starting cache refresh...');
  
  try {
    let allProducts = [];
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;
    
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
      pageCount++;
      
      // Update cache incrementally so searches work while loading
      productCache = allProducts;
      console.log(`Loaded page ${pageCount}, total products: ${allProducts.length}`);
      
      const linkHeader = response.headers.get('link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const match = linkHeader.match(/page_info=([^&>]+)/);
        pageInfo = match ? match[1] : null;
      } else {
        hasNextPage = false;
      }
      
      // Small delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    productCache = allProducts;
    totalProductsExpected = allProducts.length;
    cacheStatus = 'ready';
    console.log(`✅ Cache complete! ${allProducts.length} products loaded.`);
  } catch (error) {
    console.error('Cache refresh failed:', error);
    cacheStatus = 'error';
  } finally {
    cacheRefreshInProgress = false;
  }
}

// Test endpoint
app.get('/api/shopify', (req, res) => {
  res.json({ 
    status: 'Backend is alive!',
    cacheStatus,
    cachedProducts: productCache.length,
    totalExpected: totalProductsExpected
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
      
      // Start cache refresh in background (don't await)
      refreshProductCache(storeName, accessToken);
      
      return res.json({ 
        success: true, 
        shopName: data.shop.name,
        cacheStatus: 'loading products in background...'
      });
    }
    
    if (action === 'getProduct' && upc) {
      const searchUPC = String(upc).trim();
      
      // If cache is empty, wait a bit for it to load
      if (productCache.length === 0 && cacheStatus === 'loading') {
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
      
      // Search through cached products
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
              },
              cacheStatus,
              searchedProducts: productCache.length
            });
          }
        }
      }
      
      return res.status(404).json({ 
        error: 'Product not found',
        searchedProducts: productCache.length,
        cacheStatus,
        searchingFor: searchUPC,
        hint: cacheStatus === 'loading' ? 'Cache still loading, product might appear soon' : null
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
