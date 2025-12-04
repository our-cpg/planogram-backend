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

// Track order processing status
let orderProcessingStatus = {
  isProcessing: false,
  lastCompleted: null,
  lastResult: null
};

// Initialize database tables with SAFE error handling
async function initDatabase() {
  console.log('🔄 Initializing database...');
  
  try {
    // Create products table
    console.log('📦 Creating products table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS products (
        variant_id TEXT PRIMARY KEY,
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
      await pool.query(`ALTER TABLE products ADD COLUMN IF NOT EXISTS distributor TEXT;`);
      console.log('✅ Vendor, tags, and distributor columns added');
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
    console.log('✅ Sales data table ready');

    // Create orders table (Order Blitz)
    console.log('📋 Creating orders table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT PRIMARY KEY,
        order_number TEXT,
        customer_id BIGINT,
        customer_email_hash TEXT,
        total_price DECIMAL(10,2),
        subtotal_price DECIMAL(10,2),
        total_tax DECIMAL(10,2),
        order_date TIMESTAMPTZ,
        is_returning_customer BOOLEAN DEFAULT FALSE
      );
    `);
    console.log('✅ Orders table ready');

    // 🔥 FIX: Convert order_date to TIMESTAMPTZ to properly store UTC timestamps
    console.log('🔧 Converting order_date to TIMESTAMPTZ...');
    try {
      await pool.query(`
        ALTER TABLE orders 
        ALTER COLUMN order_date TYPE TIMESTAMPTZ USING order_date AT TIME ZONE 'UTC'
      `);
      console.log('✅ order_date converted to TIMESTAMPTZ - timezone data now preserved!');
    } catch (err) {
      console.log('⚠️ order_date conversion note:', err.message);
    }

    // Create order_items table (Order Blitz)
    console.log('📦 Creating order_items table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id BIGINT REFERENCES orders(order_id) ON DELETE CASCADE,
        variant_id TEXT,
        product_id TEXT,
        title TEXT,
        variant_title TEXT,
        quantity INTEGER,
        price DECIMAL(10,2),
        cart_position INTEGER,
        customer_is_returning BOOLEAN DEFAULT FALSE
      );
    `);
    console.log('✅ Order items table ready');
    
    // 🔥 FIX: Convert variant_id from BIGINT to TEXT in BOTH tables for custom items
    console.log('🔧 Converting variant_id to TEXT in products and order_items...');
    
    // Convert products.variant_id first
    try {
      console.log('  Converting products.variant_id to TEXT...');
      await pool.query(`
        ALTER TABLE products 
        ALTER COLUMN variant_id TYPE TEXT USING variant_id::TEXT
      `);
      console.log('  ✅ products.variant_id converted to TEXT');
    } catch (err) {
      console.error('  ❌ products.variant_id conversion failed:', err.message);
    }
    
    // Convert order_items.variant_id - MORE AGGRESSIVE APPROACH
    try {
      // Check current column type
      const typeCheck = await pool.query(`
        SELECT data_type 
        FROM information_schema.columns 
        WHERE table_name = 'order_items' AND column_name = 'variant_id'
      `);
      
      const currentType = typeCheck.rows[0]?.data_type;
      console.log(`  Current order_items.variant_id type: ${currentType}`);
      
      if (currentType !== 'text') {
        // Step 1: Drop unique constraint if it exists
        console.log('  Dropping unique constraint on order_items...');
        try {
          await pool.query(`ALTER TABLE order_items DROP CONSTRAINT IF EXISTS unique_order_variant`);
          console.log('  ✅ Constraint dropped');
        } catch (err) {
          console.log('  ⚠️ Constraint drop note:', err.message);
        }
        
        // Step 2: Change column type with USING clause
        console.log('  Converting order_items.variant_id to TEXT...');
        await pool.query(`
          ALTER TABLE order_items 
          ALTER COLUMN variant_id TYPE TEXT USING variant_id::TEXT
        `);
        console.log('  ✅ order_items.variant_id converted to TEXT');
        
        // Step 3: Recreate unique constraint
        console.log('  Recreating unique constraint...');
        try {
          await pool.query(`
            ALTER TABLE order_items 
            ADD CONSTRAINT unique_order_variant 
            UNIQUE (order_id, variant_id)
          `);
          console.log('  ✅ Constraint recreated');
        } catch (err) {
          console.log('  ⚠️ Constraint already exists or error:', err.message);
        }
      } else {
        console.log('  ✅ order_items.variant_id is already TEXT');
      }
    } catch (err) {
      console.error('  ❌ order_items.variant_id conversion failed:', err.message);
      console.error('  🔧 Attempting alternative approach - drop and recreate column...');
      
      try {
        // Last resort: Drop and recreate the column
        await pool.query(`ALTER TABLE order_items DROP COLUMN IF EXISTS variant_id`);
        await pool.query(`ALTER TABLE order_items ADD COLUMN variant_id TEXT`);
        await pool.query(`
          ALTER TABLE order_items 
          ADD CONSTRAINT unique_order_variant 
          UNIQUE (order_id, variant_id)
        `);
        console.log('  ✅ order_items.variant_id recreated as TEXT');
      } catch (recreateErr) {
        console.error('  ❌ Column recreation failed:', recreateErr.message);
      }
    }
    
    // Convert order_items.product_id to TEXT as well (for custom items)
    try {
      const typeCheck = await pool.query(`
        SELECT data_type 
        FROM information_schema.columns 
        WHERE table_name = 'order_items' AND column_name = 'product_id'
      `);
      
      const currentType = typeCheck.rows[0]?.data_type;
      console.log(`  Current order_items.product_id type: ${currentType}`);
      
      if (currentType !== 'text') {
        console.log('  Converting order_items.product_id to TEXT...');
        await pool.query(`
          ALTER TABLE order_items 
          ALTER COLUMN product_id TYPE TEXT USING product_id::TEXT
        `);
        console.log('  ✅ order_items.product_id converted to TEXT');
      } else {
        console.log('  ✅ order_items.product_id is already TEXT');
      }
    } catch (err) {
      console.error('  ❌ order_items.product_id conversion failed:', err.message);
    }

    // 🔥 FIX ALL MISSING COLUMNS FOR Order Blitz
    console.log('🔧 Fixing missing columns for Order Blitz...');
    
    // Fix missing title and variant_title columns
    try {
      await pool.query(`
        ALTER TABLE order_items 
        ADD COLUMN IF NOT EXISTS title TEXT,
        ADD COLUMN IF NOT EXISTS variant_title TEXT
      `);
      console.log('✅ Fixed order_items title columns');
    } catch (err) {
      console.log('⚠️ Order items columns note:', err.message);
    }

    // Fix missing customer_is_returning column
    try {
      await pool.query(`
        ALTER TABLE order_items 
        ADD COLUMN IF NOT EXISTS customer_is_returning BOOLEAN DEFAULT FALSE
      `);
      console.log('✅ Fixed order_items customer_is_returning column');
    } catch (err) {
      console.log('⚠️ Customer returning column note:', err.message);
    }

    // Create customer_stats table (Order Blitz)
    console.log('👥 Creating customer_stats table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_stats (
        customer_id BIGINT PRIMARY KEY,
        email_hash TEXT,
        order_count INTEGER DEFAULT 0,
        total_spent DECIMAL(10,2) DEFAULT 0,
        average_order_value DECIMAL(10,2) DEFAULT 0,
        first_order_date TIMESTAMP,
        last_order_date TIMESTAMP
      );
    `);
    console.log('✅ Customer stats table ready');

    // 🔥 FIX MISSING EMAIL_HASH COLUMN
    try {
      await pool.query(`
        ALTER TABLE customer_stats 
        ADD COLUMN IF NOT EXISTS email_hash TEXT
      `);
      console.log('✅ Fixed customer_stats email_hash column');
    } catch (err) {
      console.log('⚠️ Customer stats column note:', err.message);
    }

    // Create product_correlations table (Order Blitz) - FIXED to use variant_id
    console.log('🤝 Creating product_correlations table...');
    
    // Drop old table if it exists (schema change)
    await pool.query(`DROP TABLE IF EXISTS product_correlations;`);
    
    await pool.query(`
      CREATE TABLE IF NOT EXISTS product_correlations (
        variant_a_id TEXT,
        variant_b_id TEXT,
        co_purchase_count INTEGER DEFAULT 0,
        correlation_score DECIMAL(5,4) DEFAULT 0,
        PRIMARY KEY (variant_a_id, variant_b_id)
      );
    `);
    console.log('✅ Product correlations table ready (using variant_id for accurate tracking)');

    // Create indexes for performance
    try {
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_items_variant ON order_items(variant_id);`);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);`);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);`);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_correlations_count ON product_correlations(co_purchase_count DESC);`);
      console.log('✅ Performance indexes created');
    } catch (err) {
      console.log('⚠️ Some indexes might already exist:', err.message);
    }

    // Add unique constraint to prevent duplicate order items
    console.log('🔒 Adding unique constraint to order_items...');
    try {
      await pool.query(`
        ALTER TABLE order_items 
        ADD CONSTRAINT unique_order_variant 
        UNIQUE (order_id, variant_id)
      `);
      console.log('✅ Unique constraint added - duplicates now prevented!');
    } catch (err) {
      if (err.message.includes('already exists')) {
        console.log('✅ Unique constraint already exists');
      } else {
        console.log('⚠️ Could not add constraint:', err.message);
      }
    }

    // Create metafields table
    console.log('🏷️ Creating metafields table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS metafields (
        id SERIAL PRIMARY KEY,
        variant_id BIGINT NOT NULL,
        metafield_id BIGINT,
        namespace TEXT,
        key TEXT,
        value TEXT,
        type TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(variant_id, namespace, key)
      );
    `);
    console.log('✅ Metafields table ready');

    // Create index for metafields
    try {
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_metafields_variant ON metafields(variant_id);`);
      console.log('✅ Metafields index created');
    } catch (err) {
      console.log('⚠️ Metafields index might already exist:', err.message);
    }

    console.log('✅ Order Blitz database ready with all fixes!');
  } catch (error) {
    console.error('❌ Database init error:', error.message);
    throw error;
  }
}

// Fetch and store all Shopify products
async function fetchAllProducts(storeName, accessToken) {
  console.log('📦 Fetching all Shopify products...');
  
  try {
    let allProducts = [];
    let hasNextPage = true;
    let pageInfo = null;

    while (hasNextPage) {
      let url = `https://${storeName}/admin/api/2024-01/products.json?limit=250`;
      if (pageInfo) {
        url += `&page_info=${pageInfo}`;
      }

      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`Products API error: ${response.status}`);
      }

      const data = await response.json();
      allProducts = allProducts.concat(data.products || []);
      console.log(`Fetched ${allProducts.length} products so far...`);

      const linkHeader = response.headers.get('Link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        if (nextMatch) {
          pageInfo = nextMatch[1];
        } else {
          hasNextPage = false;
        }
      } else {
        hasNextPage = false;
      }
    }

    console.log(`✅ Fetched ${allProducts.length} total products`);

    // Store products and their variants
    let variantsStored = 0;
    for (const product of allProducts) {
      for (const variant of product.variants) {
        try {
          await pool.query(`
            INSERT INTO products (
              variant_id, product_id, title, variant_title, barcode, sku,
              price, compare_at_price, cost, inventory_quantity, vendor, tags,
              created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (variant_id) DO UPDATE SET
              title = $3, variant_title = $4, barcode = $5, sku = $6,
              price = $7, compare_at_price = $8, inventory_quantity = $10,
              vendor = $11, tags = $12, updated_at = $14
          `, [
            variant.id,
            product.id,
            product.title,
            variant.title,
            variant.barcode || '',
            variant.sku || '',
            variant.price,
            variant.compare_at_price,
            null, // Cost will be updated from inventory API
            variant.inventory_quantity,
            product.vendor || '',
            product.tags || '',
            new Date(product.created_at),
            new Date(product.updated_at)
          ]);
          variantsStored++;
        } catch (err) {
          console.error(`⚠️ Error storing variant ${variant.id}:`, err.message);
        }
      }
    }

    console.log(`✅ Stored ${variantsStored} product variants`);
    return variantsStored;

  } catch (error) {
    console.error('❌ Failed to fetch products:', error);
    throw error;
  }
}

// Fetch inventory costs and update products
async function fetchInventoryCosts(storeName, accessToken) {
  console.log('💰 Fetching inventory costs...');
  
  try {
    const result = await pool.query(`SELECT variant_id FROM products`);
    const variantIds = result.rows.map(r => r.variant_id);
    
    if (variantIds.length === 0) {
      console.log('⚠️ No variants to update');
      return 0;
    }

    console.log(`📊 Fetching costs for ${variantIds.length} variants...`);
    let updatedCount = 0;
    
    // Batch process to avoid overwhelming the API
    const batchSize = 50;
    for (let i = 0; i < variantIds.length; i += batchSize) {
      const batch = variantIds.slice(i, i + batchSize);
      
      const query = `{
        nodes(ids: [${batch.map(id => `"gid://shopify/ProductVariant/${id}"`).join(',')}]) {
          ... on ProductVariant {
            id
            inventoryItem {
              unitCost {
                amount
              }
            }
          }
        }
      }`;

      try {
        const response = await fetch(`https://${storeName}/admin/api/2024-01/graphql.json`, {
          method: 'POST',
          headers: {
            'X-Shopify-Access-Token': accessToken,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ query })
        });

        if (response.ok) {
          const data = await response.json();
          
          for (const node of (data.data?.nodes || [])) {
            if (node?.inventoryItem?.unitCost?.amount) {
              const variantId = node.id.split('/').pop();
              const cost = parseFloat(node.inventoryItem.unitCost.amount);
              
              await pool.query(
                `UPDATE products SET cost = $1 WHERE variant_id = $2`,
                [cost, variantId]
              );
              updatedCount++;
            }
          }
        }
      } catch (err) {
        console.error(`⚠️ Error fetching batch ${i}:`, err.message);
      }
      
      // Progress update
      if ((i + batchSize) % 200 === 0 || i + batchSize >= variantIds.length) {
        console.log(`📈 Progress: ${Math.min(i + batchSize, variantIds.length)}/${variantIds.length} variants processed`);
      }
    }

    console.log(`✅ Updated costs for ${updatedCount} products`);
    return updatedCount;

  } catch (error) {
    console.error('❌ Failed to fetch inventory costs:', error);
    throw error;
  }
}

// Import sales data from Shopify
async function importSalesData(storeName, accessToken) {
  console.log('📊 Starting sales data import...');
  
  try {
    // Get orders from last year for comprehensive sales data
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
    
    let allOrders = [];
    let hasNextPage = true;
    let pageInfo = null;

    console.log(`📅 Fetching orders from ${oneYearAgo.toISOString().split('T')[0]}...`);

    while (hasNextPage) {
      let url = `https://${storeName}/admin/api/2024-01/orders.json?status=any&limit=250&created_at_min=${oneYearAgo.toISOString()}`;
      if (pageInfo) {
        url = `https://${storeName}/admin/api/2024-01/orders.json?page_info=${pageInfo}`;
      }

      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`Orders API error: ${response.status}`, errorText);
        throw new Error(`Orders API error: ${response.status}`);
      }

      const data = await response.json();
      allOrders = allOrders.concat(data.orders || []);

      const linkHeader = response.headers.get('Link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        if (nextMatch) {
          pageInfo = nextMatch[1];
        } else {
          hasNextPage = false;
        }
      } else {
        hasNextPage = false;
      }
    }

    console.log(`✅ Fetched ${allOrders.length} total orders from last year`);

    // Calculate sales by variant
    const salesByVariant = new Map();
    const now = new Date();

    for (const order of allOrders) {
      const orderDate = new Date(order.created_at);
      const daysDiff = Math.floor((now - orderDate) / (1000 * 60 * 60 * 24));

      for (const lineItem of order.line_items) {
        if (!lineItem.variant_id) continue;

        const variantId = lineItem.variant_id;
        const quantity = lineItem.quantity || 1;

        if (!salesByVariant.has(variantId)) {
          salesByVariant.set(variantId, {
            daily: 0, weekly: 0, monthly: 0, quarterly: 0, yearly: 0, allTime: quantity
          });
        } else {
          const sales = salesByVariant.get(variantId);
          sales.allTime += quantity;

          if (daysDiff <= 1) sales.daily += quantity;
          if (daysDiff <= 7) sales.weekly += quantity;
          if (daysDiff <= 30) sales.monthly += quantity;
          if (daysDiff <= 90) sales.quarterly += quantity;
          if (daysDiff <= 365) sales.yearly += quantity;
        }
      }
    }

    console.log(`📊 Processing sales data for ${salesByVariant.size} variants...`);

    // Store sales data
    let updatedCount = 0;
    for (const [variantId, sales] of salesByVariant) {
      try {
        await pool.query(`
          INSERT INTO sales_data (
            variant_id, daily_sales, weekly_sales, monthly_sales,
            quarterly_sales, yearly_sales, all_time_sales, last_updated
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
          ON CONFLICT (variant_id) DO UPDATE SET
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
        updatedCount++;
      } catch (err) {
        console.error(`⚠️ Error updating sales for variant ${variantId}:`, err.message);
      }
    }

    console.log(`✅ Sales data imported for ${updatedCount} variants`);

    // Update products with zero sales
    await pool.query(`
      INSERT INTO sales_data (variant_id, daily_sales, weekly_sales, monthly_sales, quarterly_sales, yearly_sales, all_time_sales)
      SELECT variant_id::bigint, 0, 0, 0, 0, 0, 0
      FROM products p
      WHERE NOT EXISTS (SELECT 1 FROM sales_data s WHERE s.variant_id = p.variant_id::bigint)
      ON CONFLICT (variant_id) DO NOTHING
    `);

    const zeroSalesResult = await pool.query(`
      SELECT COUNT(*) as count FROM sales_data WHERE all_time_sales = 0
    `);
    const totalResult = await pool.query(`
      SELECT COUNT(*) as count FROM sales_data
    `);

    console.log(`✅ Sales data updated: ${totalResult.rows[0].count} variants with sales`);

    return { updated: updatedCount, total: salesByVariant.size };

  } catch (error) {
    console.error('❌ Failed to import sales data:', error);
    throw error;
  }
}

// 🔥 Order Blitz: Import full order data for customer analytics - FULLY FIXED
async function importOrderData(storeName, accessToken, options = {}) {
  console.log('🛒🔥 Order Blitz: Starting order data import...');
  
  try {
    let allOrders = [];
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;
    const MAX_PAGES = options.fetchAll ? 500 : 100; // More pages if fetching all

    // Use provided start date or default to 6 months ago
    let startDate;
    if (options.created_at_min) {
      startDate = new Date(options.created_at_min);
      console.log(`📅 Fetching ALL orders from ${startDate.toISOString().split('T')[0]} (custom range)...`);
    } else {
      startDate = new Date();
      startDate.setMonth(startDate.getMonth() - 6);
      console.log(`📅 Fetching orders from last 6 months (since ${startDate.toISOString().split('T')[0]})...`);
    }

    while (hasNextPage && pageCount < MAX_PAGES) {
      let url;
      if (pageInfo) {
        // When paginating, ONLY use page_info and limit
        url = `https://${storeName}/admin/api/2024-10/orders.json?page_info=${pageInfo}&limit=250`;
      } else {
        // First request: get orders from start date
        url = `https://${storeName}/admin/api/2024-10/orders.json?limit=250&status=any&created_at_min=${startDate.toISOString()}`;
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
        message: 'No orders found. Add some orders to your Shopify store to enable Order Blitz analytics!',
        analytics: null
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
          new Date(order.created_at),
          isReturning
        ]);

        // Process line items
        let cartPosition = 1;
        for (const item of order.line_items) {
          // Handle custom sale items (no variant_id/product_id)
          const isCustomSale = !item.variant_id || !item.product_id;
          
          // Use special IDs for custom items
          const variantId = isCustomSale ? `custom_${order.id}_${cartPosition}` : item.variant_id;
          const productId = isCustomSale ? `custom_${order.id}_${cartPosition}` : item.product_id;
          const itemTitle = item.title || item.name || 'Custom Sale';

          try {
            await pool.query(`
              INSERT INTO order_items (
                order_id, variant_id, product_id, title, variant_title,
                quantity, price, cart_position, customer_is_returning
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
              ON CONFLICT (order_id, variant_id) DO UPDATE SET
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                cart_position = EXCLUDED.cart_position
            `, [
              order.id,
              variantId,
              productId,
              itemTitle,
              item.variant_title || (isCustomSale ? 'Custom Item' : ''),
              item.quantity || 1,
              parseFloat(item.price) || 0,
              cartPosition++,
              isReturning
            ]);
            itemsProcessed++;
          } catch (itemErr) {
            console.log(`⚠️ Skipping problematic item in order ${order.id}:`, itemErr.message);
          }
        }

        ordersProcessed++;
        
        // Progress log every 100 orders
        if (ordersProcessed % 100 === 0) {
          console.log(`⏳ Progress: ${ordersProcessed}/${allOrders.length} orders processed...`);
        }
      } catch (orderErr) {
        console.error(`❌ Error processing order ${order.id}:`, orderErr);
      }
    }

    // Calculate customer statistics
    console.log('👥 Calculating customer statistics...');
    await pool.query(`
      INSERT INTO customer_stats (customer_id, email_hash, order_count, total_spent, average_order_value, first_order_date, last_order_date)
      SELECT 
        customer_id,
        MAX(customer_email_hash) as email_hash,
        COUNT(*) as order_count,
        SUM(total_price) as total_spent,
        AVG(total_price) as average_order_value,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date
      FROM orders
      WHERE customer_id IS NOT NULL
      GROUP BY customer_id
      ON CONFLICT (customer_id) DO UPDATE SET
        email_hash = EXCLUDED.email_hash,
        order_count = EXCLUDED.order_count,
        total_spent = EXCLUDED.total_spent,
        average_order_value = EXCLUDED.average_order_value,
        first_order_date = EXCLUDED.first_order_date,
        last_order_date = EXCLUDED.last_order_date
    `);

    // Calculate product correlations - FIXED to use variant_id for unique pairs
    console.log('🤝 Calculating product correlations (by variant for accurate tracking)...');
    await pool.query(`
      INSERT INTO product_correlations (variant_a_id, variant_b_id, co_purchase_count, correlation_score)
      SELECT 
        a.variant_id as variant_a_id,
        b.variant_id as variant_b_id,
        COUNT(*) as co_purchase_count,
        COUNT(*)::decimal / (
          SELECT COUNT(DISTINCT order_id) 
          FROM order_items 
          WHERE variant_id IN (a.variant_id, b.variant_id)
        ) as correlation_score
      FROM order_items a
      JOIN order_items b ON a.order_id = b.order_id AND a.variant_id < b.variant_id
      GROUP BY a.variant_id, b.variant_id
      HAVING COUNT(*) > 1
      ON CONFLICT (variant_a_id, variant_b_id) DO UPDATE SET
        co_purchase_count = EXCLUDED.co_purchase_count,
        correlation_score = EXCLUDED.correlation_score
    `);

    console.log(`✅🔥 Order Blitz COMPLETE: ${ordersProcessed} orders, ${itemsProcessed} items processed!`);
    console.log(`📊 Orders without customers (guest checkouts): ${ordersWithoutCustomers}`);

    // Get analytics summary
    const analyticsResult = await pool.query(`
      SELECT 
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT customer_id) as unique_customers,
        COALESCE(SUM(total_price), 0) as total_revenue,
        COALESCE(AVG(total_price), 0) as avg_order_value
      FROM orders
      WHERE order_date >= $1
    `, [startDate]);

    const analytics = analyticsResult.rows[0];

    return {
      success: true,
      ordersProcessed,
      itemsProcessed,
      ordersWithoutCustomers,
      message: `Order Blitz complete! ${ordersProcessed} orders analyzed (${ordersWithoutCustomers} guest orders).`,
      analytics: {
        totalOrders: parseInt(analytics.total_orders) || 0,
        uniqueCustomers: parseInt(analytics.unique_customers) || 0,
        totalRevenue: parseFloat(analytics.total_revenue) || 0,
        avgOrderValue: parseFloat(analytics.avg_order_value) || 0
      }
    };

  } catch (error) {
    console.error('❌ Order Blitz failed:', error);
    throw error;
  }
}

// ============================================
// BACKGROUND ORDER SYNC SYSTEM
// ============================================

/**
 * Get the most recent order date from database
 * Returns the date to start fetching from
 */
async function getLastOrderDate() {
  try {
    const result = await pool.query(`
      SELECT MAX(order_date) as last_order_date 
      FROM orders
      WHERE order_date IS NOT NULL
    `);
    
    const lastDate = result.rows[0]?.last_order_date;
    
    if (lastDate) {
      console.log(`📅 Last order in database: ${new Date(lastDate).toISOString()}`);
      // Start from 1 hour before last order to catch any late updates
      const startDate = new Date(lastDate);
      startDate.setHours(startDate.getHours() - 1);
      return startDate;
    } else {
      // No orders in database - first run, fetch last year
      console.log(`📅 No orders in database - fetching last year`);
      const oneYearAgo = new Date();
      oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
      return oneYearAgo;
    }
  } catch (error) {
    console.error('⚠️ Error getting last order date:', error);
    // Fallback to 1 day ago
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    return yesterday;
  }
}

/**
 * Background sync function - runs every 5 minutes
 */
async function backgroundOrderSync() {
  // Don't run if no settings stored
  if (!lastSettings || !lastSettings.storeName || !lastSettings.accessToken) {
    console.log('⏭️ Skipping background order sync: No Shopify credentials stored');
    return;
  }
  
  console.log('🔄 ═════════════════════════════════════════');
  console.log('🔄 BACKGROUND ORDER SYNC STARTED');
  console.log('🔄 ═════════════════════════════════════════');
  
  try {
    const startDate = await getLastOrderDate();
    const startTime = Date.now();
    
    // Use smart sync - only fetch new orders
    const options = {
      created_at_min: startDate.toISOString(),
      fetchAll: false // Don't need all pages for incremental sync
    };
    
    const result = await importOrderData(
      lastSettings.storeName, 
      lastSettings.accessToken,
      options
    );
    
    const duration = Math.round((Date.now() - startTime) / 1000);
    
    console.log('✅ ═════════════════════════════════════════');
    console.log(`✅ BACKGROUND SYNC COMPLETE (${duration}s)`);
    console.log(`📊 Processed: ${result.ordersProcessed} orders`);
    console.log(`📦 Items: ${result.itemsProcessed} line items`);
    console.log('✅ ═════════════════════════════════════════');
    
    return result;
    
  } catch (error) {
    console.error('❌ Background order sync failed:', error.message);
    console.error('Stack:', error.stack);
  }
}

/**
 * Start background sync timer
 */
const SYNC_INTERVAL = 5 * 60 * 1000; // 5 minutes
let backgroundSyncTimer = null;

function startBackgroundSync() {
  // Clear any existing timer
  if (backgroundSyncTimer) {
    clearInterval(backgroundSyncTimer);
  }
  
  console.log('⏰ Starting background order sync (every 5 minutes)...');
  
  // Run immediately on startup (after 30 second delay)
  setTimeout(() => {
    console.log('🚀 Initial background sync triggered');
    backgroundOrderSync();
  }, 30000); // 30 seconds after server starts
  
  // Then run every 5 minutes
  backgroundSyncTimer = setInterval(() => {
    console.log('⏰ Scheduled background sync triggered');
    backgroundOrderSync();
  }, SYNC_INTERVAL);
  
  console.log('✅ Background sync scheduled');
}


// ============================================
// ROOT ROUTES (Health Check & API Info)
// ============================================

// Root route - Health check
app.get('/', (req, res) => {
  res.json({ 
    status: 'ok',
    service: 'Planogram Backend v2.0',
    version: '2.0.0',
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

// API info route
app.get('/api', (req, res) => {
  res.json({
    service: 'Planogram Backend API',
    version: '2.0',
    endpoints: [
      'GET / - Health check',
      'GET /api - API info',
      'POST /api/shopify - Main Shopify operations',
      'GET /api/orders/status - Order processing status',
      'GET /api/products/all - Get all products',
      'GET /api/stats - Database stats',
      'GET /api/correlations - Product correlations',
      'GET /api/product/:upc - Get product by UPC',
      'GET /api/debug/order/:orderNumber - Debug specific order',
      'POST /api/cleanup/duplicates - Remove duplicate orders',
      'POST /api/settings - Store settings for auto-refresh'
    ]
  });
});

// ============================================
// Main API endpoint
app.post('/api/shopify', async (req, res) => {
  const { storeName, accessToken, action } = req.body;

  if (!storeName || !accessToken) {
    return res.status(400).json({ error: 'Store name and access token required' });
  }

  try {
    console.log(`📥 API Request: ${action}`);

    if (action === 'testConnection') {
      // Simple test - just verify we can connect to Shopify
      console.log('🧪 Testing Shopify connection...');
      
      try {
        const testUrl = `https://${storeName}/admin/api/2024-01/shop.json`;
        const testResponse = await fetch(testUrl, {
          headers: {
            'X-Shopify-Access-Token': accessToken,
            'Content-Type': 'application/json'
          }
        });

        if (testResponse.ok) {
          const shopData = await testResponse.json();
          return res.json({
            success: true,
            message: `Connected to ${shopData.shop.name}!`,
            shopName: shopData.shop.name,
            domain: shopData.shop.domain
          });
        } else {
          const errorText = await testResponse.text();
          return res.status(400).json({
            success: false,
            error: 'Invalid credentials or store name',
            details: errorText
          });
        }
      } catch (error) {
        return res.status(400).json({
          success: false,
          error: 'Failed to connect to Shopify',
          details: error.message
        });
      }

    } else if (action === 'refreshInventory') {
      const productCount = await fetchAllProducts(storeName, accessToken);
      const costCount = await fetchInventoryCosts(storeName, accessToken);
      const salesData = await importSalesData(storeName, accessToken);
      
      res.json({ 
        success: true, 
        productsUpdated: productCount,
        costsUpdated: costCount,
        salesUpdated: salesData.updated,
        message: `Inventory refreshed: ${productCount} products, ${costCount} costs, ${salesData.updated} sales records`
      });

    } else if (action === 'refreshOrders') {
      console.log('🔥🛒 Order Blitz ORDER REFRESH REQUESTED...');
      
      if (orderProcessingStatus.isProcessing) {
        return res.json({
          success: false,
          message: 'Order processing already in progress. Please wait.',
          processing: true
        });
      }
      
      orderProcessingStatus.isProcessing = true;
      
      // Start processing in background (don't await)
      // Pass date parameters from frontend request
      const importOptions = {
        created_at_min: req.body.created_at_min,
        created_at_max: req.body.created_at_max,
        fetchAll: req.body.fetchAll
      };
      importOrderData(storeName, accessToken, importOptions)
        .then(result => {
          console.log(`✅ Background processing complete: ${result.ordersProcessed} orders`);
          orderProcessingStatus.isProcessing = false;
          orderProcessingStatus.lastCompleted = new Date();
          orderProcessingStatus.lastResult = result;
        })
        .catch(error => {
          console.error('❌ Background processing failed:', error);
          orderProcessingStatus.isProcessing = false;
        });
      
      // Return immediately
      res.json({ 
        success: true, 
        message: 'Order processing started in background. Check /api/orders/status for progress.',
        processing: true
      });

    } else if (action === 'getOrderAnalytics') {
      // NEW ENDPOINT: Get order analytics data for display
      console.log('📊 Fetching order analytics for display...');
      
      try {
        // Get overall stats
        const overallStats = await pool.query(`
          SELECT 
            COUNT(DISTINCT order_id) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            COALESCE(SUM(total_price), 0) as total_sales,
            COALESCE(AVG(total_price), 0) as avg_order_value
          FROM orders
        `);

        // Get recent orders for display
        const recentOrders = await pool.query(`
          SELECT 
            o.order_id,
            o.order_number,
            o.customer_email_hash,
            o.total_price,
            o.order_date,
            o.is_returning_customer,
            COALESCE(SUM(oi.quantity), 0) as item_count
          FROM orders o
          LEFT JOIN order_items oi ON o.order_id = oi.order_id
          GROUP BY o.order_id, o.order_number, o.customer_email_hash, o.total_price, o.order_date, o.is_returning_customer
          ORDER BY o.order_date DESC
          LIMIT 50
        `);

        // Get sales by time period - ADJUSTED FOR DENVER TIMEZONE (MST/MDT = UTC-7)
        const now = new Date();
        
        // Calculate "today" in Denver time (UTC-7)
        // Convert current UTC time to Denver time, then get midnight
        const denverOffset = -7 * 60; // MST/MDT offset in minutes
        const denverNow = new Date(now.getTime() + denverOffset * 60 * 1000);
        const denverMidnight = new Date(denverNow.getFullYear(), denverNow.getMonth(), denverNow.getDate());
        // Convert back to UTC for database comparison
        const todayStart = new Date(denverMidnight.getTime() - denverOffset * 60 * 1000);
        
        const weekStart = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
        const yearStart = new Date(now.getFullYear(), 0, 1);
        
        console.log(`📅 Date range - Today starts at: ${todayStart.toISOString()} (Denver midnight in UTC)`);
        console.log(`📅 Current UTC time: ${now.toISOString()}`);
        console.log(`📅 Denver local time: ~${denverNow.toISOString().replace('T', ' ').split('.')[0]}`);


        const salesByPeriod = await pool.query(`
          SELECT 
            COUNT(CASE WHEN order_date >= $1 THEN 1 END) as today_orders,
            COALESCE(SUM(CASE WHEN order_date >= $1 THEN total_price END), 0) as today_sales,
            COUNT(CASE WHEN order_date >= $2 THEN 1 END) as week_orders,
            COALESCE(SUM(CASE WHEN order_date >= $2 THEN total_price END), 0) as week_sales,
            COUNT(CASE WHEN order_date >= $3 THEN 1 END) as month_orders,
            COALESCE(SUM(CASE WHEN order_date >= $3 THEN total_price END), 0) as month_sales,
            COUNT(CASE WHEN order_date >= $4 THEN 1 END) as year_orders,
            COALESCE(SUM(CASE WHEN order_date >= $4 THEN total_price END), 0) as year_sales
          FROM orders
        `, [todayStart, weekStart, monthStart, yearStart]);

        // Get top products by sales
        const topProducts = await pool.query(`
          SELECT 
            p.title,
            p.variant_title,
            p.barcode,
            COUNT(DISTINCT oi.order_id) as times_ordered,
            SUM(oi.quantity) as total_quantity,
            SUM(oi.price * oi.quantity) as total_revenue
          FROM order_items oi
          JOIN products p ON p.variant_id = oi.variant_id
          GROUP BY p.title, p.variant_title, p.barcode
          ORDER BY total_revenue DESC
          LIMIT 10
        `);

        const stats = overallStats.rows[0];
        const periods = salesByPeriod.rows[0];

        res.json({
          success: true,
          totalOrders: parseInt(stats.total_orders) || 0,
          uniqueCustomers: parseInt(stats.unique_customers) || 0,
          totalSales: parseFloat(stats.total_sales) || 0,
          avgOrderValue: parseFloat(stats.avg_order_value) || 0,
          recentOrders: await Promise.all(recentOrders.rows.map(async (order) => {
            // Fetch REAL line items for this order
            const lineItemsResult = await pool.query(`
              SELECT 
                title,
                variant_title,
                quantity,
                price,
                variant_id as sku
              FROM order_items
              WHERE order_id = $1
              ORDER BY cart_position
            `, [order.order_id]);
            
            return {
              id: order.order_id,
              order_number: order.order_number,
              created_at: order.order_date,
              total_price: parseFloat(order.total_price).toFixed(2),
              financial_status: order.total_price > 0 ? 'paid' : 'pending',
              customer: {
                first_name: 'Guest',
                last_name: ''
              },
              // Return REAL line items with all data
              line_items: lineItemsResult.rows.map(item => ({
                title: item.title,
                variant_title: item.variant_title,
                quantity: item.quantity,
                price: parseFloat(item.price),
                sku: item.sku
              }))
            };
          })),
          salesByPeriod: {
            today: {
              orders: parseInt(periods.today_orders) || 0,
              sales: parseFloat(periods.today_sales) || 0
            },
            week: {
              orders: parseInt(periods.week_orders) || 0,
              sales: parseFloat(periods.week_sales) || 0
            },
            month: {
              orders: parseInt(periods.month_orders) || 0,
              sales: parseFloat(periods.month_sales) || 0
            },
            year: {
              orders: parseInt(periods.year_orders) || 0,
              sales: parseFloat(periods.year_sales) || 0
            }
          },
          topProducts: topProducts.rows.map(p => ({
            name: p.variant_title ? `${p.title} - ${p.variant_title}` : p.title,
            barcode: p.barcode,
            timesOrdered: parseInt(p.times_ordered),
            totalQuantity: parseInt(p.total_quantity),
            totalRevenue: parseFloat(p.total_revenue)
          })),
          lastRefresh: orderProcessingStatus.lastCompleted,
          processingStatus: orderProcessingStatus.isProcessing ? 'Processing...' : 'Idle'
        });
      } catch (error) {
        console.error('❌ Error fetching order analytics:', error);
        res.status(500).json({ 
          success: false, 
          error: 'Failed to fetch order analytics',
          message: error.message 
        });
      }

    } else if (action === 'refreshProducts') {
      // This is likely a typo in your frontend - should be refreshInventory
      console.log('⚠️ refreshProducts called - redirecting to refreshInventory');
      const productCount = await fetchAllProducts(storeName, accessToken);
      const costCount = await fetchInventoryCosts(storeName, accessToken);
      const salesData = await importSalesData(storeName, accessToken);
      
      res.json({ 
        success: true, 
        productsUpdated: productCount,
        costsUpdated: costCount,
        salesUpdated: salesData.updated,
        message: `Inventory refreshed: ${productCount} products, ${costCount} costs, ${salesData.updated} sales records`
      });
      
    } else if (action === 'getOrders') {
      // Return raw orders from database for frontend analysis
      console.log('📦 Fetching all orders for frontend analysis...');
      
      try {
        const ordersResult = await pool.query(`
          SELECT 
            order_id as id,
            order_number,
            total_price,
            order_date as created_at,
            customer_id,
            is_returning_customer
          FROM orders
          ORDER BY order_date DESC
        `);
        
        console.log(`✅ Returning ${ordersResult.rows.length} orders for analysis`);
        
        res.json({
          success: true,
          orders: ordersResult.rows.map(row => ({
            id: row.id,
            order_number: row.order_number,
            total_price: row.total_price,
            created_at: row.created_at,
            customer_id: row.customer_id
          }))
        });
      } catch (error) {
        console.error('❌ Error fetching orders:', error);
        res.status(500).json({ error: 'Failed to fetch orders' });
      }
      
    } else {
      res.status(400).json({ error: 'Invalid action' });
    }

  } catch (error) {
    console.error('❌ API Error:', error);
    console.error('Stack:', error.stack);
    res.status(500).json({ error: error.message });
  }
});

// CLEANUP ENDPOINT: Remove duplicate order items
app.post('/api/cleanup/duplicates', async (req, res) => {
  try {
    console.log('🧹 Starting duplicate cleanup...');
    
    // First, check how many duplicates exist
    const duplicateCheck = await pool.query(`
      SELECT 
        oi.id,
        oi.order_id,
        oi.variant_id,
        o.order_number
      FROM order_items oi
      JOIN orders o ON o.order_id = oi.order_id
      WHERE oi.id NOT IN (
        SELECT MIN(id)
        FROM order_items
        GROUP BY order_id, variant_id
      )
    `);
    
    const duplicateCount = duplicateCheck.rows.length;
    console.log(`⚠️ Found ${duplicateCount} duplicate rows`);
    
    if (duplicateCount === 0) {
      return res.json({
        success: true,
        message: 'No duplicates found!',
        duplicatesRemoved: 0
      });
    }
    
    // Delete duplicates (keeps first occurrence of each order+variant)
    const deleteResult = await pool.query(`
      DELETE FROM order_items
      WHERE id NOT IN (
        SELECT MIN(id)
        FROM order_items
        GROUP BY order_id, variant_id
      )
    `);
    
    console.log(`✅ Removed ${deleteResult.rowCount} duplicate rows`);
    
    // Verify the cleanup
    const verification = await pool.query(`
      SELECT 
        COUNT(DISTINCT order_id) as affected_orders,
        COUNT(*) as remaining_items
      FROM order_items
    `);
    
    res.json({
      success: true,
      message: `Cleanup complete! Removed ${deleteResult.rowCount} duplicate items.`,
      duplicatesRemoved: deleteResult.rowCount,
      affectedOrders: parseInt(verification.rows[0].affected_orders),
      remainingItems: parseInt(verification.rows[0].remaining_items)
    });
    
  } catch (error) {
    console.error('❌ Cleanup error:', error);
    res.status(500).json({ 
      success: false,
      error: error.message 
    });
  }
});

// DEBUG ENDPOINT: Check order item details
app.get('/api/debug/order/:orderNumber', async (req, res) => {
  const { orderNumber } = req.params;
  
  try {
    console.log(`🔍 Debugging order #${orderNumber}...`);
    
    // Get the raw order_items data
    const items = await pool.query(`
      SELECT 
        oi.id,
        oi.variant_id,
        oi.title,
        oi.variant_title,
        oi.quantity
      FROM order_items oi
      JOIN orders o ON o.order_id = oi.order_id
      WHERE o.order_number = $1
      ORDER BY oi.id
    `, [orderNumber]);
    
    // Calculate totals
    const totalQuantity = items.rows.reduce((sum, item) => sum + parseInt(item.quantity || 0), 0);
    const rowCount = items.rows.length;
    
    // Check for duplicates
    const variantCounts = {};
    items.rows.forEach(item => {
      const key = item.variant_id;
      variantCounts[key] = (variantCounts[key] || 0) + 1;
    });
    const duplicates = Object.entries(variantCounts).filter(([_, count]) => count > 1);
    
    console.log(`📊 Order #${orderNumber}: ${rowCount} rows, ${totalQuantity} total items`);
    
    res.json({
      orderNumber: orderNumber,
      rowCount: rowCount,
      totalQuantity: totalQuantity,
      hasDuplicates: duplicates.length > 0,
      duplicateVariants: duplicates.map(([variantId, count]) => ({
        variantId,
        appearsCount: count
      })),
      items: items.rows,
      quantities: items.rows.map(i => i.quantity)
    });
    
  } catch (error) {
    console.error('❌ Debug error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get product by UPC
app.get('/api/product/:upc', async (req, res) => {
  const { upc } = req.params;
  
  if (!upc) {
    return res.status(400).json({ error: 'UPC required' });
  }

  try {
    console.log(`🔍 Looking up product: ${upc}`);
    
    const result = await pool.query(`
      SELECT 
        p.*,
        s.daily_sales,
        s.weekly_sales,
        s.monthly_sales,
        s.quarterly_sales,
        s.yearly_sales,
        s.all_time_sales
      FROM products p
      LEFT JOIN sales_data s ON p.variant_id = s.variant_id::TEXT
      WHERE p.barcode = $1
      LIMIT 1
    `, [upc]);

    if (result.rows.length === 0) {
      console.log(`❌ Product not found: ${upc}`);
      return res.status(404).json({ error: 'Product not found' });
    }

    const product = result.rows[0];
    console.log(`✅ Found: ${product.title}`);

    // 🔥 Order Blitz: Get analytics data
    const analyticsResult = await pool.query(`
      SELECT 
        COUNT(DISTINCT oi.order_id) as times_purchased,
        COALESCE(AVG(o.total_price), 0) as average_order_value,
        COALESCE(SUM(CASE WHEN o.is_returning_customer THEN oi.quantity ELSE 0 END), 0) as returning_customer_purchases,
        COALESCE(SUM(CASE WHEN NOT o.is_returning_customer THEN oi.quantity ELSE 0 END), 0) as new_customer_purchases,
        COALESCE(AVG(oi.cart_position), 0) as average_cart_position
      FROM order_items oi
      JOIN orders o ON o.order_id = oi.order_id
      WHERE oi.variant_id = $1
    `, [product.variant_id]);

    const analytics = analyticsResult.rows[0] || {};

    // Get products bought together (using variant_id for accurate tracking)
    const correlationsResult = await pool.query(`
      SELECT 
        p.title as product_name,
        p.variant_title,
        pc.co_purchase_count,
        pc.correlation_score
      FROM product_correlations pc
      JOIN products p ON p.variant_id = pc.variant_b_id
      WHERE pc.variant_a_id = $1
      ORDER BY pc.co_purchase_count DESC, pc.correlation_score DESC
      LIMIT 5
    `, [product.variant_id]);

    const boughtTogether = correlationsResult.rows.map(r => ({
      name: r.variant_title ? `${r.product_name} - ${r.variant_title}` : r.product_name,
      count: parseInt(r.co_purchase_count),
      score: parseFloat(r.correlation_score)
    }));

    res.json({
      productId: product.product_id,
      variantId: product.variant_id,
      title: product.title,
      variantTitle: product.variant_title,
      price: parseFloat(product.price) || 0,
      compareAtPrice: parseFloat(product.compare_at_price) || null,
      cost: parseFloat(product.cost) || null,
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
      // 🔥 Order Blitz Analytics
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
    res.status(500).json({ error: 'Database error' });
  }
});

// Get order processing status
app.get('/api/orders/status', (req, res) => {
  res.json({
    isProcessing: orderProcessingStatus.isProcessing,
    lastCompleted: orderProcessingStatus.lastCompleted,
    lastResult: orderProcessingStatus.lastResult ? {
      ordersProcessed: orderProcessingStatus.lastResult.ordersProcessed,
      itemsProcessed: orderProcessingStatus.lastResult.itemsProcessed,
      analytics: orderProcessingStatus.lastResult.analytics
    } : null
  });
});

// Get all products with forecasting data
app.get('/api/products/all', async (req, res) => {
  try {
    console.log('📊 Fetching all products with forecasting data...');
    
    const result = await pool.query(`
      SELECT 
        p.variant_id,
        p.product_id,
        p.title,
        p.variant_title,
        p.barcode,
        p.sku,
        p.price,
        p.cost,
        p.inventory_quantity,
        p.vendor,
        p.tags,
        COALESCE(s.daily_sales, 0) as daily_sales,
        COALESCE(s.weekly_sales, 0) as weekly_sales,
        COALESCE(s.monthly_sales, 0) as monthly_sales,
        COALESCE(s.all_time_sales, 0) as all_time_sales
      FROM products p
      LEFT JOIN sales_data s ON p.variant_id = s.variant_id::TEXT
      ORDER BY p.title, p.variant_title
    `);
    
    const products = result.rows.map(row => {
      const stock = row.inventory_quantity !== null && row.inventory_quantity !== undefined 
        ? parseInt(row.inventory_quantity) 
        : 0;
      const velocity = parseFloat(row.daily_sales) || 0;
      const daysLeft = velocity > 0 ? Math.floor(stock / velocity) : (stock > 0 ? 999 : 0);
      
      // Determine risk level
      let risk = 'LOW';
      if (daysLeft < 0) risk = 'CRITICAL';
      else if (daysLeft <= 3) risk = 'CRITICAL';
      else if (daysLeft <= 7) risk = 'HIGH';
      else if (daysLeft <= 14) risk = 'MEDIUM';
      
      return {
        variantId: row.variant_id,
        productId: row.product_id,
        title: row.title,
        variantTitle: row.variant_title,
        barcode: row.barcode,
        sku: row.sku,
        price: parseFloat(row.price) || 0,
        cost: parseFloat(row.cost) || 0,
        stock: stock,
        velocity: velocity,
        daysLeft: daysLeft,
        risk: risk,
        vendor: row.vendor,
        tags: row.tags,
        dailySales: parseInt(row.daily_sales) || 0,
        weeklySales: parseInt(row.weekly_sales) || 0,
        monthlySales: parseInt(row.monthly_sales) || 0,
        allTimeSales: parseInt(row.all_time_sales) || 0
      };
    });
    
    // Calculate summary stats
    const criticalCount = products.filter(p => p.daysLeft <= 3).length;
    const highRiskCount = products.filter(p => p.daysLeft > 3 && p.daysLeft <= 7).length;
    const totalProducts = products.length;
    const avgVelocity = products.reduce((sum, p) => sum + p.velocity, 0) / totalProducts;
    
    console.log(`✅ Returned ${totalProducts} products`);
    console.log(`📊 Critical: ${criticalCount}, High Risk: ${highRiskCount}`);
    
    res.json({
      products: products,
      summary: {
        totalProducts: totalProducts,
        criticalAlerts: criticalCount,
        highRisk: highRiskCount,
        avgVelocity: avgVelocity.toFixed(2)
      }
    });
    
  } catch (error) {
    console.error('❌ Error fetching products:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get stats
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

// Get top product correlations
app.get('/api/correlations', async (req, res) => {
  try {
    console.log('🤝 Fetching top product correlations...');
    
    // First, let's see the distribution of counts
    const statsResult = await pool.query(`
      SELECT 
        co_purchase_count,
        COUNT(*) as num_pairs
      FROM product_correlations
      GROUP BY co_purchase_count
      ORDER BY co_purchase_count DESC
      LIMIT 10
    `);
    console.log('📊 Top co-purchase counts:', statsResult.rows);
    
    const result = await pool.query(`
      SELECT 
        pa.title as product_a_name,
        pa.variant_title as product_a_variant,
        pa.barcode as product_a_upc,
        pa.price as product_a_price,
        pb.title as product_b_name,
        pb.variant_title as product_b_variant,
        pb.barcode as product_b_upc,
        pb.price as product_b_price,
        pc.co_purchase_count,
        pc.correlation_score
      FROM product_correlations pc
      JOIN products pa ON pa.variant_id = pc.variant_a_id
      JOIN products pb ON pb.variant_id = pc.variant_b_id
      ORDER BY pc.co_purchase_count DESC, pc.correlation_score DESC
      LIMIT 50
    `);

    const correlations = result.rows.map(r => ({
      productA: {
        name: r.product_a_variant ? `${r.product_a_name} - ${r.product_a_variant}` : r.product_a_name,
        upc: r.product_a_upc,
        price: parseFloat(r.product_a_price)
      },
      productB: {
        name: r.product_b_variant ? `${r.product_b_name} - ${r.product_b_variant}` : r.product_b_name,
        upc: r.product_b_upc,
        price: parseFloat(r.product_b_price)
      },
      timesBoughtTogether: parseInt(r.co_purchase_count),
      correlationScore: parseFloat(r.correlation_score)
    }));

    console.log(`✅ Returning ${correlations.length} correlations`);
    console.log(`📦 Sample: ${correlations[0]?.productA.name} + ${correlations[0]?.productB.name} = ${correlations[0]?.timesBoughtTogether} times`);
    if (correlations.length > 10) {
      console.log(`📦 Sample #10: ${correlations[10]?.productA.name} + ${correlations[10]?.productB.name} = ${correlations[10]?.timesBoughtTogether} times`);
    }
    
    res.json({ correlations });
  } catch (error) {
    console.error('❌ Correlations error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ============================================
// DATABASE ORDER ENDPOINTS
// ============================================

// Get orders from database (no Shopify API call)
app.get('/api/orders/from-database', async (req, res) => {
  try {
    console.log('📊 Fetching orders from database...');
    
    const { limit = 20000 } = req.query;
    
    // Get orders
    const ordersResult = await pool.query(`
      SELECT 
        o.order_id,
        o.order_number,
        o.customer_id,
        o.total_price,
        o.order_date,
        o.is_returning_customer
      FROM orders o
      ORDER BY o.order_date DESC
      LIMIT $1
    `, [parseInt(limit)]);
    
    const orderIds = ordersResult.rows.map(o => o.order_id);
    
    if (orderIds.length === 0) {
      return res.json({ 
        orders: [], 
        totalOrders: 0,
        source: 'database',
        message: 'No orders in database. Waiting for background sync...' 
      });
    }
    
    // Get line items for these orders
    const itemsResult = await pool.query(`
      SELECT 
        order_id,
        variant_id,
        product_id,
        title,
        variant_title,
        quantity,
        price
      FROM order_items
      WHERE order_id = ANY($1::bigint[])
    `, [orderIds]);
    
    // Group line items by order
    const itemsByOrder = {};
    for (const item of itemsResult.rows) {
      if (!itemsByOrder[item.order_id]) {
        itemsByOrder[item.order_id] = [];
      }
      itemsByOrder[item.order_id].push({
        id: item.variant_id,
        variant_id: item.variant_id,
        product_id: item.product_id,
        name: item.title,
        title: item.title,
        variant_title: item.variant_title,
        quantity: item.quantity,
        price: parseFloat(item.price)
      });
    }
    
    // Combine orders with their line items
    const orders = ordersResult.rows.map(order => ({
      id: order.order_id,
      order_number: order.order_number,
      customer: order.customer_id ? { id: order.customer_id } : null,
      total_price: parseFloat(order.total_price),
      created_at: order.order_date,
      line_items: itemsByOrder[order.order_id] || []
    }));
    
    console.log(`✅ Returning ${orders.length} orders from database`);
    
    res.json({ 
      orders,
      totalOrders: orders.length,
      source: 'database',
      lastSync: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Error fetching orders from database:', error);
    res.status(500).json({ error: error.message });
  }
});

// Manually trigger background sync
app.post('/api/orders/sync-now', async (req, res) => {
  try {
    console.log('🔄 Manual sync triggered');
    
    // Run in background (don't wait)
    backgroundOrderSync();
    
    res.json({ 
      success: true, 
      message: 'Background sync triggered. Check /api/orders/sync-status for progress.' 
    });
    
  } catch (error) {
    console.error('❌ Manual sync error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Check sync status
app.get('/api/orders/sync-status', async (req, res) => {
  try {
    // Get last order date from database
    const lastOrderResult = await pool.query(`
      SELECT 
        MAX(order_date) as last_order_date,
        COUNT(*) as total_orders
      FROM orders
    `);
    
    const lastOrder = lastOrderResult.rows[0];
    
    res.json({
      lastSyncedOrder: lastOrder.last_order_date,
      totalOrdersInDatabase: parseInt(lastOrder.total_orders),
      nextSyncIn: Math.round(SYNC_INTERVAL / 1000), // seconds
      syncInterval: '5 minutes',
      hasCredentials: !!(lastSettings && lastSettings.storeName && lastSettings.accessToken)
    });
    
  } catch (error) {
    console.error('❌ Sync status error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Start server
async function startServer() {
  console.log('🚀 Starting Planogram Backend v2.0 (Sales Tracking + Order Blitz)...');
  await initDatabase();
  app.listen(PORT, () => {
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`📊 Sales tracking: ENABLED`);
    console.log(`🔥 Order Blitz: READY`);
    console.log(`⏰ Background order sync: STARTING...`);
    console.log(`🔥 ALL DATABASE FIXES: APPLIED`);
    console.log(`🔗 Test at: http://localhost:${PORT}`);
    
    // Start background sync after server is up
    startBackgroundSync();
  });
}

// Auto-refresh functionality
const AUTO_REFRESH_INTERVAL = 60 * 60 * 1000; // 1 hour
let activeConnections = [];
let lastSettings = null;

// Store settings
app.post('/api/settings', (req, res) => {
  const { storeName, accessToken } = req.body;
  if (!storeName || !accessToken) {
    return res.status(400).json({ error: 'Invalid settings' });
  }
  
  lastSettings = { storeName, accessToken };
  console.log('✅ Settings stored for auto-refresh');
  
  res.json({ success: true, message: 'Settings stored' });
});

// Auto-refresh function
async function autoRefreshInventory() {
  if (!lastSettings) {
    console.log('⏭️ Skipping auto-refresh: No settings stored');
    return;
  }
  
  console.log('🔄 Starting auto-refresh...');
  
  try {
    const productCount = await fetchAllProducts(lastSettings.storeName, lastSettings.accessToken);
    const salesData = await importSalesData(lastSettings.storeName, lastSettings.accessToken);
    
    const message = `Auto-refresh complete: ${productCount} products, ${salesData.updated} sales records`;
    console.log(`✅ ${message}`);
    
    // Notify connected clients
    activeConnections.forEach(conn => {
      conn.write(`data: ${JSON.stringify({ type: 'refresh', message })}\n\n`);
    });
  } catch (error) {
    console.error('❌ Auto-refresh failed:', error);
  }
}

// Set up auto-refresh interval
setInterval(autoRefreshInventory, AUTO_REFRESH_INTERVAL);

startServer();
