import express from 'express';
import cors from 'cors';
import pg from 'pg';
import crypto from 'crypto';

const app = express();
const PORT = process.env.PORT || 10000;
const { Pool } = pg;

// Database connection - SAME AS YOUR ORIGINAL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

// CORS middleware - SAME AS YOUR ORIGINAL
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

// Initialize database tables with SAFE error handling
async function initDatabase() {
  console.log('🔄 Initializing database...');
  
  try {
    // Create products table
    console.log('📦 Creating products table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS products (
        variant_id BIGINT PRIMARY KEY,
        product_id BIGINT NOT NULL,
        title TEXT NOT NULL,
        variant_title TEXT,
        barcode TEXT,
        sku TEXT,
        price DECIMAL(10,2),
        compare_at_price DECIMAL(10,2),
        cost DECIMAL(10,2),
        inventory_quantity INTEGER,
        vendor TEXT,
        tags TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
      );
    `);
    console.log('✅ Products table ready');

    // Add vendor and tags columns if they don't exist
    try {
      await pool.query(`ALTER TABLE products ADD COLUMN IF NOT EXISTS vendor TEXT;`);
      await pool.query(`ALTER TABLE products ADD COLUMN IF NOT EXISTS tags TEXT;`);
      console.log('✅ Vendor and tags columns added');
    } catch (err) {
      console.log('⚠️ Columns might already exist:', err.message);
    }

    // Create index
    console.log('📑 Creating barcode index...');
    try {
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_barcode ON products(barcode);`);
      console.log('✅ Index ready');
    } catch (err) {
      console.log('⚠️ Index already exists or error:', err.message);
    }

    // Create sales_data table with ALL columns
    console.log('📊 Creating sales_data table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS sales_data (
        variant_id BIGINT PRIMARY KEY,
        daily_sales INTEGER DEFAULT 0,
        weekly_sales INTEGER DEFAULT 0,
        monthly_sales INTEGER DEFAULT 0,
        quarterly_sales INTEGER DEFAULT 0,
        yearly_sales INTEGER DEFAULT 0,
        all_time_sales INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT NOW()
      );
    `);
    console.log('✅ Sales_data table ready');

    // Try to add new columns (in case table exists from old version)
    console.log('🔧 Checking for new columns...');
    const columnsToAdd = [
      'daily_sales',
      'weekly_sales', 
      'quarterly_sales',
      'yearly_sales',
      'all_time_sales'
    ];

    for (const column of columnsToAdd) {
      try {
        await pool.query(`
          ALTER TABLE sales_data 
          ADD COLUMN IF NOT EXISTS ${column} INTEGER DEFAULT 0;
        `);
        console.log(`✅ Column ${column} ready`);
      } catch (err) {
        console.log(`⚠️ Column ${column} might already exist:`, err.message);
      }
    }

    console.log('✅✅✅ Database initialization complete!');
    
    // 🔥 NEW: Create orders table for full order tracking
    console.log('🛒 Creating orders table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT PRIMARY KEY,
        order_number TEXT,
        customer_id BIGINT,
        customer_email_hash TEXT,
        total_price DECIMAL(10,2),
        subtotal_price DECIMAL(10,2),
        total_tax DECIMAL(10,2),
        order_date TIMESTAMP,
        is_returning_customer BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT NOW()
      );
    `);
    console.log('✅ Orders table ready');

    // Create order_items table for product sequences
    console.log('📦 Creating order_items table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id BIGINT NOT NULL,
        variant_id BIGINT NOT NULL,
        product_id BIGINT NOT NULL,
        quantity INTEGER,
        price DECIMAL(10,2),
        cart_position INTEGER,
        created_at TIMESTAMP DEFAULT NOW(),
        FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
      );
    `);
    console.log('✅ Order_items table ready');

    // Create customer_stats table
    console.log('👥 Creating customer_stats table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_stats (
        customer_id BIGINT PRIMARY KEY,
        customer_email_hash TEXT,
        order_count INTEGER DEFAULT 0,
        total_spent DECIMAL(10,2) DEFAULT 0,
        average_order_value DECIMAL(10,2) DEFAULT 0,
        first_order_date TIMESTAMP,
        last_order_date TIMESTAMP,
        updated_at TIMESTAMP DEFAULT NOW()
      );
    `);
    console.log('✅ Customer_stats table ready');

    // Create product_correlations table for "bought together" analysis
    console.log('🤝 Creating product_correlations table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS product_correlations (
        id SERIAL PRIMARY KEY,
        product_a_id BIGINT NOT NULL,
        product_b_id BIGINT NOT NULL,
        co_purchase_count INTEGER DEFAULT 0,
        correlation_score DECIMAL(5,4),
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(product_a_id, product_b_id)
      );
    `);
    console.log('✅ Product_correlations table ready');

    // Create indexes for better query performance
    console.log('📑 Creating indexes...');
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_customer ON orders(customer_id);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_date ON orders(order_date);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_items_variant ON order_items(variant_id);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_correlation_product_a ON product_correlations(product_a_id);`);
    console.log('✅ All indexes ready');

    console.log('🔥🔥🔥 BEAST MODE DATABASE READY!');
  } catch (error) {
    console.error('❌ Database init error:', error);
    console.error('Stack:', error.stack);
  }
}

// Your original import products function
async function importProducts(storeName, accessToken) {
  console.log('🔄 Starting product import from Shopify...');
  console.log('📋 Filtering: Only products with proper title case (not ALL CAPS)');
  
  try {
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;
    let totalInserted = 0;
    let totalSkipped = 0;

    const isProperlyFormatted = (title) => {
      if (title === title.toUpperCase()) return false;
      if (!/[a-z]/.test(title)) return false;
      return true;
    };

    const cleanStoreName = (name) => {
      return name.replace('.myshopify.com', '');
    };

    const storeNameClean = cleanStoreName(storeName);

    while (hasNextPage) {
      const url = pageInfo 
        ? `https://${storeNameClean}.myshopify.com/admin/api/2025-10/products.json?limit=250&page_info=${pageInfo}`
        : `https://${storeNameClean}.myshopify.com/admin/api/2025-10/products.json?limit=250`;

      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`Shopify API error: ${response.status}`);
      }

      const data = await response.json();
      const products = data.products || [];
      
      pageCount++;
      console.log(`📦 Fetched page ${pageCount}: ${products.length} products`);

      for (const product of products) {
        if (!isProperlyFormatted(product.title)) {
          totalSkipped++;
          continue;
        }

        for (const variant of product.variants) {
          try {
            await pool.query(`
              INSERT INTO products (
                variant_id, product_id, title, variant_title, barcode, sku, 
                price, compare_at_price, cost, inventory_quantity, 
                vendor, tags, created_at, updated_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
              ON CONFLICT (variant_id) 
              DO UPDATE SET
                title = EXCLUDED.title,
                variant_title = EXCLUDED.variant_title,
                barcode = EXCLUDED.barcode,
                sku = EXCLUDED.sku,
                price = EXCLUDED.price,
                compare_at_price = EXCLUDED.compare_at_price,
                cost = EXCLUDED.cost,
                inventory_quantity = EXCLUDED.inventory_quantity,
                vendor = EXCLUDED.vendor,
                tags = EXCLUDED.tags,
                updated_at = EXCLUDED.updated_at
            `, [
              variant.id,
              product.id,
              product.title,
              variant.title !== 'Default Title' ? variant.title : null,
              variant.barcode || null,
              variant.sku || null,
              parseFloat(variant.price) || 0,
              variant.compare_at_price ? parseFloat(variant.compare_at_price) : null,
              variant.inventory_management ? parseFloat(variant.price) * 0.6 : null,
              variant.inventory_quantity || 0,
              product.vendor || null,
              product.tags ? product.tags.join(', ') : null,
              product.created_at,
              product.updated_at
            ]);
            
            totalInserted++;
          } catch (err) {
            console.error(`❌ Error inserting variant ${variant.id}:`, err.message);
          }
        }
      }

      console.log(`✅ Page ${pageCount}: ${totalInserted} inserted, ${totalSkipped} skipped`);

      const linkHeader = response.headers.get('Link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        pageInfo = nextMatch ? nextMatch[1] : null;
        hasNextPage = !!pageInfo;
      } else {
        hasNextPage = false;
      }

      await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log(`✅ Import complete! ${totalInserted} products inserted, ${totalSkipped} skipped`);
    return { 
      success: true, 
      message: `Successfully imported ${totalInserted} products (${totalSkipped} skipped)`,
      total: totalInserted,
      skipped: totalSkipped
    };

  } catch (error) {
    console.error('❌ Import failed:', error);
    throw error;
  }
}

// UPDATED: Import sales data for multiple time periods
async function importSalesData(storeName, accessToken) {
  console.log('📊 Starting sales data import from Shopify Orders...');
  
  try {
    const now = new Date();
    const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    const oneMonthAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    const threeMonthsAgo = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
    const oneYearAgo = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
    
    const storeNameClean = storeName.replace('.myshopify.com', '');

    let allOrders = [];
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;

    console.log('📥 Fetching orders from last year...');

    while (hasNextPage) {
      const url = pageInfo 
        ? `https://${storeNameClean}.myshopify.com/admin/api/2025-10/orders.json?limit=250&status=any&created_at_min=${oneYearAgo.toISOString()}&page_info=${pageInfo}`
        : `https://${storeNameClean}.myshopify.com/admin/api/2025-10/orders.json?limit=250&status=any&created_at_min=${oneYearAgo.toISOString()}`;

      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorBody = await response.text();
        console.error(`❌ Orders API failed with ${response.status} on page ${pageCount}`);
        console.error(`Response: ${errorBody}`);
        
        // If we already got some orders, break the loop and process what we have
        if (allOrders.length > 0) {
          console.log(`⚠️ Pagination failed, but we have ${allOrders.length} orders. Processing what we got...`);
          hasNextPage = false;
          break;
        }
        
        throw new Error(`Shopify Orders API error: ${response.status} - ${errorBody}`);
      }

      const data = await response.json();
      allOrders = allOrders.concat(data.orders || []);
      pageCount++;
      console.log(`📦 Fetched page ${pageCount}: ${data.orders?.length || 0} orders`);

      const linkHeader = response.headers.get('Link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        pageInfo = nextMatch ? nextMatch[1] : null;
        hasNextPage = !!pageInfo;
      } else {
        hasNextPage = false;
      }

      await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log(`✅ Fetched ${allOrders.length} total orders from last year`);

    // Calculate sales for each time period
    const salesByVariant = {};
    
    allOrders.forEach(order => {
      const orderDate = new Date(order.created_at);
      
      order.line_items?.forEach(item => {
        const variantId = item.variant_id;
        if (!variantId) return;

        if (!salesByVariant[variantId]) {
          salesByVariant[variantId] = {
            daily: 0,
            weekly: 0,
            monthly: 0,
            quarterly: 0,
            yearly: 0,
            allTime: 0
          };
        }

        const qty = item.quantity;
        salesByVariant[variantId].allTime += qty;
        
        if (orderDate >= oneDayAgo) salesByVariant[variantId].daily += qty;
        if (orderDate >= oneWeekAgo) salesByVariant[variantId].weekly += qty;
        if (orderDate >= oneMonthAgo) salesByVariant[variantId].monthly += qty;
        if (orderDate >= threeMonthsAgo) salesByVariant[variantId].quarterly += qty;
        if (orderDate >= oneYearAgo) salesByVariant[variantId].yearly += qty;
      });
    });

    console.log(`📊 Processing sales data for ${Object.keys(salesByVariant).length} variants...`);

    let updated = 0;
    for (const [variantId, sales] of Object.entries(salesByVariant)) {
      try {
        await pool.query(`
          INSERT INTO sales_data (
            variant_id, 
            daily_sales, 
            weekly_sales, 
            monthly_sales, 
            quarterly_sales, 
            yearly_sales, 
            all_time_sales,
            last_updated
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
          ON CONFLICT (variant_id) 
          DO UPDATE SET 
            daily_sales = $2,
            weekly_sales = $3,
            monthly_sales = $4,
            quarterly_sales = $5,
            yearly_sales = $6,
            all_time_sales = $7,
            last_updated = NOW()
        `, [
          variantId, 
          sales.daily, 
          sales.weekly, 
          sales.monthly, 
          sales.quarterly, 
          sales.yearly, 
          sales.allTime
        ]);
        updated++;
      } catch (err) {
        console.error(`❌ Error updating sales for variant ${variantId}:`, err.message);
      }
    }

    console.log(`✅ Sales data imported for ${updated} variants`);
    
    return { 
      success: true, 
      message: 'Sales data imported for all time periods',
      variantsUpdated: updated
    };

  } catch (error) {
    console.error('❌ Sales import failed:', error);
    console.error('Stack:', error.stack);
    throw error;
  }
}

// 🔥 BEAST MODE: Import full order data for customer analytics
async function importOrderData(storeName, accessToken) {
  console.log('🛒🔥 BEAST MODE: Starting order data import...');
  
  try {
    let allOrders = [];
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;
    const MAX_PAGES = 50; // Limit to prevent infinite loops (50 pages * 250 = 12,500 orders)

    // Fetch orders from Shopify - get ALL orders from the beginning
    console.log('📅 Fetching ALL orders (including historical)...');
    while (hasNextPage && pageCount < MAX_PAGES) {
      let url;
      if (pageInfo) {
        // When paginating, ONLY use page_info (no other params)
        url = `https://${storeName}/admin/api/2024-10/orders.json?limit=250&page_info=${pageInfo}`;
      } else {
        // First request: get all orders from 2020 onwards
        url = `https://${storeName}/admin/api/2024-10/orders.json?limit=250&created_at_min=2020-01-01`;
      }

      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`❌ Orders API failed: ${response.status}`, errorText);
        throw new Error(`Orders API error: ${response.status} - ${errorText}`);
      }

      const data = await response.json();
      allOrders = allOrders.concat(data.orders || []);
      pageCount++;
      console.log(`📦 Fetched page ${pageCount}: ${data.orders?.length || 0} orders (Total so far: ${allOrders.length})`);

      const linkHeader = response.headers.get('Link');
      console.log(`🔗 Link header:`, linkHeader);
      
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        if (nextMatch) {
          pageInfo = nextMatch[1];
          console.log(`➡️ Found next page with pageInfo: ${pageInfo.substring(0, 20)}...`);
        } else {
          console.log(`⚠️ Has 'next' in link header but couldn't parse page_info`);
          hasNextPage = false;
        }
      } else {
        console.log(`✅ No more pages - this was the last page`);
        hasNextPage = false;
      }
    }

    console.log(`🎯 Fetched ${allOrders.length} total orders`);

    // Handle stores with no orders
    if (allOrders.length === 0) {
      console.log('ℹ️ No orders found in store');
      return {
        success: true,
        ordersProcessed: 0,
        itemsProcessed: 0,
        ordersWithoutCustomers: 0,
        message: 'No orders found. Add some orders to your Shopify store to enable BEAST MODE analytics!'
      };
    }

    // Process each order
    let ordersProcessed = 0;
    let itemsProcessed = 0;
    let ordersWithoutCustomers = 0;
    const customerOrderCounts = new Map();

    for (const order of allOrders) {
      try {
        const customerId = order.customer?.id || null;
        const emailHash = order.customer?.email ? 
          crypto.createHash('md5').update(order.customer.email).digest('hex') : null;

        // Track orders without customers
        if (!customerId) {
          ordersWithoutCustomers++;
        }

        // Track customer order count (only if customer exists)
        if (customerId) {
          customerOrderCounts.set(customerId, (customerOrderCounts.get(customerId) || 0) + 1);
        }

        const isReturning = customerId ? (customerOrderCounts.get(customerId) > 1) : false;

        // Insert/Update order
        await pool.query(`
          INSERT INTO orders (
            order_id, order_number, customer_id, customer_email_hash,
            total_price, subtotal_price, total_tax, order_date, is_returning_customer
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          ON CONFLICT (order_id) DO UPDATE SET
            total_price = EXCLUDED.total_price,
            is_returning_customer = EXCLUDED.is_returning_customer
        `, [
          order.id,
          order.order_number || order.name,
          customerId,
          emailHash,
          parseFloat(order.total_price) || 0,
          parseFloat(order.subtotal_price) || 0,
          parseFloat(order.total_tax) || 0,
          order.created_at,
          isReturning
        ]);

        ordersProcessed++;

        // Insert order items (skip items without variant_id like custom items, gift cards)
        if (order.line_items && order.line_items.length > 0) {
          for (let i = 0; i < order.line_items.length; i++) {
            const item = order.line_items[i];
            
            // Skip items without variant_id (custom line items, gift cards, etc)
            if (!item.variant_id || !item.product_id) {
              console.log(`⚠️ Skipping line item without variant_id in order ${order.id}`);
              continue;
            }
            
            await pool.query(`
              INSERT INTO order_items (
                order_id, variant_id, product_id, quantity, price, cart_position
              ) VALUES ($1, $2, $3, $4, $5, $6)
              ON CONFLICT DO NOTHING
            `, [
              order.id,
              item.variant_id,
              item.product_id,
              item.quantity || 1,
              parseFloat(item.price) || 0,
              i + 1 // Cart position (1st product, 2nd product, etc.)
            ]);

            itemsProcessed++;
          }
        }

        // Update customer stats
        if (customerId) {
          await pool.query(`
            INSERT INTO customer_stats (
              customer_id, customer_email_hash, order_count, total_spent, 
              first_order_date, last_order_date
            ) 
            SELECT 
              $1, $2, 
              COUNT(*), 
              SUM(total_price),
              MIN(order_date),
              MAX(order_date)
            FROM orders WHERE customer_id = $1
            ON CONFLICT (customer_id) DO UPDATE SET
              order_count = EXCLUDED.order_count,
              total_spent = EXCLUDED.total_spent,
              average_order_value = EXCLUDED.total_spent / EXCLUDED.order_count,
              last_order_date = EXCLUDED.last_order_date,
              updated_at = NOW()
          `, [customerId, emailHash]);
        }

      } catch (err) {
        console.error(`❌ Error processing order ${order.id}:`, err.message);
      }
    }

    // Calculate product correlations (which products are bought together)
    console.log('🤝 Calculating product correlations...');
    await pool.query(`
      INSERT INTO product_correlations (product_a_id, product_b_id, co_purchase_count, correlation_score)
      SELECT 
        a.product_id as product_a_id,
        b.product_id as product_b_id,
        COUNT(DISTINCT a.order_id) as co_purchase_count,
        (COUNT(DISTINCT a.order_id)::DECIMAL / 
          (SELECT COUNT(DISTINCT order_id) FROM order_items WHERE product_id = a.product_id)) as correlation_score
      FROM order_items a
      JOIN order_items b ON a.order_id = b.order_id AND a.product_id < b.product_id
      GROUP BY a.product_id, b.product_id
      HAVING COUNT(DISTINCT a.order_id) >= 2
      ON CONFLICT (product_a_id, product_b_id) DO UPDATE SET
        co_purchase_count = EXCLUDED.co_purchase_count,
        correlation_score = EXCLUDED.correlation_score,
        updated_at = NOW()
    `);

    console.log(`✅🔥 BEAST MODE COMPLETE: ${ordersProcessed} orders, ${itemsProcessed} items processed!`);
    console.log(`📊 Orders without customers (guest checkouts): ${ordersWithoutCustomers}`);

    return {
      success: true,
      ordersProcessed,
      itemsProcessed,
      ordersWithoutCustomers,
      message: `Beast mode engaged! ${ordersProcessed} orders analyzed (${ordersWithoutCustomers} guest orders).`
    };

  } catch (error) {
    console.error('❌ Order import failed:', error);
    throw error;
  }
}

// API Endpoints

app.get('/', (req, res) => {
  res.json({ 
    status: 'Planogram Backend Running - SALES TRACKING ENABLED', 
    timestamp: new Date().toISOString(),
    version: '2.0-sales-tracking'
  });
});

app.post('/api/shopify', async (req, res) => {
  const { storeName, accessToken, action } = req.body;

  console.log(`📥 API Request: ${action}`);

  if (!storeName || !accessToken) {
    return res.status(400).json({ error: 'Store name and access token required' });
  }

  try {
    if (action === 'connect') {
      const storeNameClean = storeName.replace('.myshopify.com', '');
      const testUrl = `https://${storeNameClean}.myshopify.com/admin/api/2025-10/shop.json`;
      
      console.log(`🔌 Testing connection to ${storeNameClean}...`);
      
      const response = await fetch(testUrl, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        console.error(`❌ Connection failed: ${response.status}`);
        return res.status(401).json({ error: 'Invalid Shopify credentials' });
      }

      const data = await response.json();
      console.log(`✅ Connected to shop: ${data.shop.name}`);
      
      res.json({ 
        success: true, 
        shop: data.shop.name,
        message: 'Connected to Shopify successfully'
      });

    } else if (action === 'refreshCache') {
      console.log('🔄 FULL REFRESH REQUESTED - Products + Sales...');
      
      console.log('Step 1/2: Importing products...');
      await importProducts(storeName, accessToken);
      
      console.log('Step 2/2: Importing sales data...');
      await importSalesData(storeName, accessToken);
      
      const countResult = await pool.query('SELECT COUNT(*) as count FROM products');
      const productCount = parseInt(countResult.rows[0].count);
      
      console.log(`✅✅✅ REFRESH COMPLETE! ${productCount} products with sales data ready`);
      
      res.json({ 
        success: true, 
        message: `Full refresh complete! ${productCount} products with sales data`,
        productCount
      });

    } else if (action === 'refreshProducts') {
      console.log('📦 PRODUCTS REFRESH REQUESTED...');
      
      await importProducts(storeName, accessToken);
      
      const countResult = await pool.query('SELECT COUNT(*) as count FROM products');
      const productCount = parseInt(countResult.rows[0].count);
      
      console.log(`✅ Products imported: ${productCount}`);
      
      res.json({ 
        success: true, 
        message: `${productCount} products imported`,
        productCount
      });

    } else if (action === 'refreshSales') {
      console.log('📊 SALES REFRESH REQUESTED...');
      
      await importSalesData(storeName, accessToken);
      
      const salesResult = await pool.query('SELECT COUNT(*) as count FROM sales_data WHERE monthly_sales > 0');
      const salesCount = parseInt(salesResult.rows[0].count);
      
      console.log(`✅ Sales data updated: ${salesCount} variants with sales`);
      
      res.json({ 
        success: true, 
        message: `Sales updated! ${salesCount} products with sales data`,
        salesCount
      });

    } else if (action === 'refreshOrders') {
      console.log('🔥🛒 BEAST MODE ORDER REFRESH REQUESTED...');
      
      const result = await importOrderData(storeName, accessToken);
      
      res.json({ 
        success: true, 
        message: result.message,
        ordersProcessed: result.ordersProcessed,
        itemsProcessed: result.itemsProcessed
      });

    } else {
      res.status(400).json({ error: 'Invalid action' });
    }

  } catch (error) {
    console.error('❌ API Error:', error);
    console.error('Stack:', error.stack);
    res.status(500).json({ error: error.message, stack: error.stack });
  }
});

// UPDATED: Return sales data for all time periods
app.get('/api/product/:upc', async (req, res) => {
  const { upc } = req.params;
  
  console.log(`🔍 Searching for UPC: "${upc}"`);

  try {
    const result = await pool.query(`
      SELECT 
        p.*,
        COALESCE(s.daily_sales, 0) as daily_sales,
        COALESCE(s.weekly_sales, 0) as weekly_sales,
        COALESCE(s.monthly_sales, 0) as monthly_sales,
        COALESCE(s.quarterly_sales, 0) as quarterly_sales,
        COALESCE(s.yearly_sales, 0) as yearly_sales,
        COALESCE(s.all_time_sales, 0) as all_time_sales
      FROM products p
      LEFT JOIN sales_data s ON p.variant_id = s.variant_id
      WHERE p.barcode = $1
      LIMIT 1
    `, [upc]);

    if (result.rows.length === 0) {
      console.log(`❌ No product found with barcode: "${upc}"`);
      return res.status(404).json({ error: 'Product not found' });
    }

    const product = result.rows[0];
    
    // 🔥 BEAST MODE: Get analytics data
    const analyticsQuery = await pool.query(`
      SELECT 
        COUNT(DISTINCT oi.order_id) as times_purchased,
        AVG(o.total_price) as average_order_value,
        SUM(CASE WHEN o.is_returning_customer THEN 1 ELSE 0 END) as returning_customer_purchases,
        SUM(CASE WHEN NOT o.is_returning_customer THEN 1 ELSE 0 END) as new_customer_purchases,
        AVG(oi.cart_position) as average_cart_position
      FROM order_items oi
      JOIN orders o ON oi.order_id = o.order_id
      WHERE oi.variant_id = $1
    `, [product.variant_id]);
    
    const analytics = analyticsQuery.rows[0] || {};
    
    // Get top 3 products bought together
    const correlationsQuery = await pool.query(`
      SELECT 
        p.title,
        p.variant_title,
        pc.co_purchase_count,
        pc.correlation_score
      FROM product_correlations pc
      JOIN products p ON (p.product_id = pc.product_b_id OR p.product_id = pc.product_a_id)
      WHERE (pc.product_a_id = $1 OR pc.product_b_id = $1)
        AND p.product_id != $1
      ORDER BY pc.co_purchase_count DESC
      LIMIT 3
    `, [product.product_id]);
    
    const boughtTogether = correlationsQuery.rows.map(r => ({
      name: r.variant_title ? `${r.title} - ${r.variant_title}` : r.title,
      count: parseInt(r.co_purchase_count),
      score: parseFloat(r.correlation_score)
    }));

    console.log(`✅ Found: ${product.title}`);
    console.log(`📊 Sales: Day=${product.daily_sales}, Week=${product.weekly_sales}, Month=${product.monthly_sales}`);
    console.log(`📦 Stock: ${product.inventory_quantity} units`);
    console.log(`💰 AOV: $${analytics.average_order_value || 0}`);

    res.json({
      title: product.title,
      variantTitle: product.variant_title,
      price: parseFloat(product.price),
      compareAtPrice: product.compare_at_price ? parseFloat(product.compare_at_price) : null,
      cost: product.cost ? parseFloat(product.cost) : parseFloat(product.price) * 0.6,
      dailySales: parseInt(product.daily_sales) || 0,
      weeklySales: parseInt(product.weekly_sales) || 0,
      monthlySales: parseInt(product.monthly_sales) || 0,
      quarterlySales: parseInt(product.quarterly_sales) || 0,
      yearlySales: parseInt(product.yearly_sales) || 0,
      allTimeSales: parseInt(product.all_time_sales) || 0,
      barcode: product.barcode,
      sku: product.sku,
      inventoryQuantity: parseInt(product.inventory_quantity) || 0,
      vendor: product.vendor,
      tags: product.tags,
      createdAt: product.created_at,
      updatedAt: product.updated_at,
      // 🔥 BEAST MODE Analytics
      analytics: {
        timesPurchased: parseInt(analytics.times_purchased) || 0,
        averageOrderValue: parseFloat(analytics.average_order_value) || 0,
        returningCustomerPurchases: parseInt(analytics.returning_customer_purchases) || 0,
        newCustomerPurchases: parseInt(analytics.new_customer_purchases) || 0,
        averageCartPosition: parseFloat(analytics.average_cart_position) || 0,
        boughtTogether: boughtTogether
      }
    });

  } catch (error) {
    console.error('❌ Database error:', error);
    console.error('Stack:', error.stack);
    res.status(500).json({ error: 'Database query failed', details: error.message });
  }
});

app.get('/api/stats', async (req, res) => {
  try {
    const productCount = await pool.query('SELECT COUNT(*) as count FROM products');
    const salesCount = await pool.query('SELECT COUNT(*) as count FROM sales_data WHERE monthly_sales > 0');
    const totalSales = await pool.query('SELECT SUM(monthly_sales) as total FROM sales_data');
    
    res.json({
      products: parseInt(productCount.rows[0].count),
      productsWithSales: parseInt(salesCount.rows[0].count),
      totalMonthlySales: parseInt(totalSales.rows[0].total) || 0
    });
  } catch (error) {
    console.error('❌ Stats error:', error);
    res.status(500).json({ error: error.message });
  }
});

async function startServer() {
  console.log('🚀 Starting Planogram Backend v2.0 (Sales Tracking)...');
  await initDatabase();
  app.listen(PORT, () => {
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`📊 Sales tracking: ENABLED`);
    console.log(`🔗 Test at: http://localhost:${PORT}`);
  });
}

pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('❌ Database connection failed:', err);
  } else {
    console.log('✅ Database connected successfully');
    console.log(`📅 Server time: ${res.rows[0].now}`);
  }
});

startServer();
