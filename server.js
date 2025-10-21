import express from 'express';
import cors from 'cors';
import pg from 'pg';

const app = express();
const PORT = process.env.PORT || 10000;
const { Pool } = pg;

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

// CORS middleware
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Max-Age', '86400');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  next();
});

app.use(express.json());

// Initialize database tables
async function initDatabase() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS products (
        id BIGINT PRIMARY KEY,
        title TEXT NOT NULL,
        variant_id BIGINT NOT NULL,
        variant_title TEXT,
        barcode TEXT,
        sku TEXT,
        price DECIMAL(10,2),
        compare_at_price DECIMAL(10,2),
        cost DECIMAL(10,2),
        inventory_quantity INTEGER,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
      );
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_barcode ON products(barcode);
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sales_data (
        variant_id BIGINT PRIMARY KEY,
        monthly_sales INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT NOW()
      );
    `);

    console.log('✅ Database tables initialized');
  } catch (error) {
    console.error('❌ Database init error:', error);
  }
}

// Import products from Shopify (MEMORY EFFICIENT + TITLE FILTER)
async function importProducts(storeName, accessToken) {
  console.log('🔄 Starting product import from Shopify...');
  console.log('📋 Filtering: Only products with proper title case (not ALL CAPS)');
  
  try {
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;
    let totalInserted = 0;
    let totalSkipped = 0;

    // Clear existing products first
    await pool.query('TRUNCATE TABLE products CASCADE');
    console.log('🗑️ Cleared existing products');

    // Helper function to check if title is properly formatted
    const isProperlyFormatted = (title) => {
      // Skip if title is all uppercase (like "WAVING BUTTER POMADE")
      if (title === title.toUpperCase()) {
        return false;
      }
      
      // Must have at least one lowercase letter (like "Waving Butter Pomade")
      if (!/[a-z]/.test(title)) {
        return false;
      }
      
      return true;
    };

    // Fetch and insert in batches
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
        throw new Error('Failed to fetch products from Shopify');
      }
      
      const data = await response.json();
      pageCount++;
      
      // Insert this batch immediately (with title filter!)
      for (const product of data.products) {
        // CHECK: Skip products with improperly formatted titles
        if (!isProperlyFormatted(product.title)) {
          totalSkipped++;
          continue; // Skip this product entirely
        }
        
        for (const variant of product.variants || []) {
          try {
            await pool.query(`
              INSERT INTO products (
                id, title, variant_id, variant_title, barcode, sku,
                price, compare_at_price, cost, inventory_quantity,
                created_at, updated_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            `, [
              variant.id,
              product.title,
              variant.id,
              variant.title,
              variant.barcode || null,
              variant.sku || null,
              parseFloat(variant.price),
              parseFloat(variant.compare_at_price || 0),
              parseFloat(variant.compare_at_price || variant.price * 0.5),
              variant.inventory_quantity || 0,
              product.created_at,
              product.updated_at
            ]);
            totalInserted++;
          } catch (error) {
            console.error(`Error inserting variant ${variant.id}:`, error.message);
          }
        }
      }
      
      if (pageCount % 10 === 0) {
        console.log(`  Page ${pageCount}: ✅ ${totalInserted} imported, ⏭️ ${totalSkipped} skipped (ALL CAPS)`);
      }
      
      const linkHeader = response.headers.get('link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const match = linkHeader.match(/page_info=([^&>]+)/);
        pageInfo = match ? match[1] : null;
      } else {
        hasNextPage = false;
      }
      
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    console.log(`✅ Imported ${totalInserted} product variants`);
    console.log(`⏭️ Skipped ${totalSkipped} products (ALL CAPS titles)`);
    return totalInserted;
    
  } catch (error) {
    console.error('❌ Product import error:', error);
    throw error;
  }
}

// Fetch sales data from Shopify Orders API
async function importSalesData(storeName, accessToken) {
  console.log('🔄 Starting sales data import from Shopify Orders...');
  
  try {
    let allOrders = [];
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;

    // Get orders from last 30 days
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    const dateFilter = thirtyDaysAgo.toISOString();

    while (hasNextPage && pageCount < 20) { // Limit to 5000 orders (20 pages)
      const url = pageInfo 
        ? `https://${storeName}/admin/api/2024-01/orders.json?limit=250&status=any&page_info=${pageInfo}`
        : `https://${storeName}/admin/api/2024-01/orders.json?limit=250&status=any&created_at_min=${dateFilter}`;
        
      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });
      
      if (!response.ok) {
        throw new Error('Failed to fetch orders from Shopify');
      }
      
      const data = await response.json();
      allOrders = allOrders.concat(data.orders);
      pageCount++;
      
      console.log(`  Fetched page ${pageCount}: ${allOrders.length} orders so far...`);
      
      const linkHeader = response.headers.get('link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const match = linkHeader.match(/page_info=([^&>]+)/);
        pageInfo = match ? match[1] : null;
      } else {
        hasNextPage = false;
      }
      
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    console.log(`✅ Fetched ${allOrders.length} orders from last 30 days`);
    console.log('📊 Calculating sales velocity...');

    // Count sales per variant
    const salesCount = {};
    for (const order of allOrders) {
      for (const item of order.line_items || []) {
        const variantId = item.variant_id;
        if (variantId) {
          salesCount[variantId] = (salesCount[variantId] || 0) + item.quantity;
        }
      }
    }

    // Clear existing sales data
    await pool.query('TRUNCATE TABLE sales_data');

    // Insert sales data
    let insertCount = 0;
    for (const [variantId, quantity] of Object.entries(salesCount)) {
      try {
        await pool.query(`
          INSERT INTO sales_data (variant_id, monthly_sales, last_updated)
          VALUES ($1, $2, NOW())
          ON CONFLICT (variant_id) DO UPDATE SET
            monthly_sales = EXCLUDED.monthly_sales,
            last_updated = NOW()
        `, [parseInt(variantId), quantity]);
        insertCount++;
      } catch (error) {
        console.error(`Error inserting sales for variant ${variantId}:`, error.message);
      }
    }

    console.log(`✅ Imported sales data for ${insertCount} products`);
    return insertCount;
    
  } catch (error) {
    console.error('❌ Sales import error:', error);
    throw error;
  }
}

// API Routes
app.get('/api/shopify', async (req, res) => {
  try {
    const result = await pool.query('SELECT COUNT(*) as count FROM products');
    const count = parseInt(result.rows[0].count);
    
    res.json({ 
      status: 'Backend is alive!',
      database: 'connected',
      productsInDatabase: count
    });
  } catch (error) {
    res.json({ 
      status: 'Backend is alive!',
      database: 'error',
      error: error.message
    });
  }
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
      
      return res.json({ 
        success: true, 
        shopName: data.shop.name,
        message: 'Connected! Use "Refresh Cache" to import products.'
      });
    }

    if (action === 'refreshCache') {
      console.log('📦 Full sync requested: Products + Sales Data');
      
      // Import products
      const productCount = await importProducts(storeName, accessToken);
      
      // Import sales data
      const salesCount = await importSalesData(storeName, accessToken);
      
      return res.json({ 
        success: true, 
        message: 'Database synced successfully',
        productsLoaded: productCount,
        salesDataLoaded: salesCount
      });
    }
    
    if (action === 'getProduct' && upc) {
      const searchUPC = String(upc).trim();
      
      console.log(`🔍 Searching database for UPC: "${searchUPC}"`);
      
      const result = await pool.query(`
        SELECT p.*, s.monthly_sales
        FROM products p
        LEFT JOIN sales_data s ON p.variant_id = s.variant_id
        WHERE p.barcode = $1
        LIMIT 1
      `, [searchUPC]);
      
      if (result.rows.length > 0) {
        const product = result.rows[0];
        console.log(`✅ Found product: ${product.title}`);
        
        return res.json({
          success: true,
          product: {
            name: `${product.title}${product.variant_title && product.variant_title !== 'Default Title' ? ' - ' + product.variant_title : ''}`,
            price: parseFloat(product.price),
            cost: parseFloat(product.cost || product.price * 0.5),
            monthlySales: product.monthly_sales || 0,
            sku: product.sku
          }
        });
      }
      
      console.log(`❌ No product found with barcode: "${searchUPC}"`);
      
      return res.status(404).json({ 
        error: 'Product not found in database',
        hint: 'Click "Refresh Cache" to sync latest products from Shopify'
      });
    }
    
    return res.json({ message: 'Backend ready' });
    
  } catch (error) {
    console.error('Error:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Initialize and start server
async function startServer() {
  await initDatabase();
  
  app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`📊 Database connected`);
  });
}

startServer();
