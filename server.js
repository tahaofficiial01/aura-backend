require('dotenv').config();
const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const cors = require('cors');
const bodyParser = require('body-parser');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(bodyParser.json());

// Request Logger
app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
    next();
});

// Database Connection
let db;

// Initialize database
const initDb = async () => {
    db = await open({
        filename: './aura_inventory.db',
        driver: sqlite3.Database
    });

    // Run initialization queries to create tables if they don't exist
    await db.exec(`
        PRAGMA foreign_keys = ON;
        
        CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            sku TEXT,
            purchase_price REAL NOT NULL,
            sale_price REAL NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0,
            category TEXT,
            shop_id TEXT,
            supplier_id TEXT,
            supplier_name TEXT,
            size TEXT,
            unit TEXT,
            created_at INTEGER NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS customers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            phone TEXT,
            address TEXT,
            balance REAL NOT NULL DEFAULT 0,
            total_purchased REAL NOT NULL DEFAULT 0,
            total_paid REAL NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS sales (
            id TEXT PRIMARY KEY,
            type TEXT DEFAULT 'Sale',
            total REAL NOT NULL,
            created_at INTEGER NOT NULL,
            customer_id TEXT,
            customer_name TEXT,
            customer_phone TEXT,
            customer_address TEXT,
            payment_type TEXT,
            amount_paid REAL NOT NULL DEFAULT 0,
            remaining_balance REAL NOT NULL DEFAULT 0,
            due_date INTEGER,
            shop_id TEXT
        );
        
        CREATE TABLE IF NOT EXISTS sale_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            name TEXT NOT NULL,
            sku TEXT,
            quantity INTEGER NOT NULL,
            sale_price REAL NOT NULL,
            total REAL NOT NULL,
            unit TEXT,
            size TEXT,
            FOREIGN KEY (sale_id) REFERENCES sales(id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS payments (
            id TEXT PRIMARY KEY,
            customer_id TEXT,
            sale_id TEXT,
            amount REAL NOT NULL,
            created_at INTEGER NOT NULL,
            method TEXT,
            note TEXT
        );
        
        CREATE TABLE IF NOT EXISTS expenses (
            id TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            amount REAL NOT NULL,
            category TEXT,
            created_at INTEGER NOT NULL,
            shop_id TEXT
        );
        
        CREATE TABLE IF NOT EXISTS suppliers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            contact TEXT,
            alternate_phone TEXT,
            shop_name TEXT,
            address TEXT,
            notes TEXT,
            balance REAL NOT NULL DEFAULT 0,
            total_purchased REAL NOT NULL DEFAULT 0,
            total_paid REAL NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            next_payment_date INTEGER
        );
        
        CREATE TABLE IF NOT EXISTS purchases (
            id TEXT PRIMARY KEY,
            supplier_id TEXT NOT NULL,
            supplier_name TEXT,
            total_amount REAL NOT NULL,
            paid_amount REAL NOT NULL DEFAULT 0,
            remaining_amount REAL NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            shop_id TEXT
        );
        
        CREATE TABLE IF NOT EXISTS purchase_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            cost_price REAL NOT NULL,
            total REAL NOT NULL,
            unit TEXT,
            size TEXT,
            FOREIGN KEY (purchase_id) REFERENCES purchases(id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS supplier_payments (
            id TEXT PRIMARY KEY,
            supplier_id TEXT NOT NULL,
            amount REAL NOT NULL,
            created_at INTEGER NOT NULL,
            method TEXT,
            note TEXT
        );
        
        CREATE INDEX IF NOT EXISTS idx_sales_customer_id ON sales(customer_id);
        CREATE INDEX IF NOT EXISTS idx_payments_customer_id ON payments(customer_id);
        CREATE INDEX IF NOT EXISTS idx_purchases_supplier_id ON purchases(supplier_id);
        CREATE INDEX IF NOT EXISTS idx_supplier_payments_supplier_id ON supplier_payments(supplier_id);
    `);
};

// Initialize database
initDb();

// Helper to generate robust unique IDs (UUID v4)
const generateId = () => crypto.randomUUID();

// --- PRODUCTS API ---

app.get('/api/products', async (req, res) => {
    try {
        const rows = await db.all('SELECT * FROM products ORDER BY created_at DESC');
        const products = rows.map(p => ({
            ...p,
            purchasePrice: Number(p.purchase_price),
            salePrice: Number(p.sale_price),
            stock: Number(p.stock),
            createdAt: Number(p.created_at),
            shopId: p.shop_id,
            supplierId: p.supplier_id,
            supplierName: p.supplier_name
        }));
        res.json(products);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/products', async (req, res) => {
    const { name, sku, purchasePrice, salePrice, stock, category, shopId, supplierId, supplierName, size, unit } = req.body;
    const id = generateId();
    const createdAt = Date.now();
    try {
        await db.run(
            'INSERT INTO products (id, name, sku, purchase_price, sale_price, stock, category, shop_id, supplier_id, supplier_name, size, unit, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [id, name, sku, purchasePrice, salePrice, stock, category, shopId, supplierId, supplierName, size, unit, createdAt]
        );
        res.status(201).json({ id, ...req.body, createdAt });
    } catch (error) {
        console.error('Error adding product:', error);
        res.status(500).json({ error: error.message });
    }
});

app.put('/api/products/:id', async (req, res) => {
    const { name, sku, purchasePrice, salePrice, stock, category, supplierId, supplierName, size, unit } = req.body;
    try {
        await db.run(
            'UPDATE products SET name=?, sku=?, purchase_price=?, sale_price=?, stock=?, category=?, supplier_id=?, supplier_name=?, size=?, unit=? WHERE id=?',
            [name, sku, purchasePrice, salePrice, stock, category, supplierId, supplierName, size, unit, req.params.id]
        );
        res.json({ message: 'Product updated' });
    } catch (error) {
        console.error('Error updating product:', error);
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/products/:id', async (req, res) => {
    try {
        await db.run('DELETE FROM products WHERE id=?', [req.params.id]);
        res.json({ message: 'Product deleted' });
    } catch (error) {
        console.error('Error deleting product:', error);
        res.status(500).json({ error: error.message });
    }
});

// Transfer Stock
app.post('/api/products/transfer', async (req, res) => {
    const { sourceProductId, quantity } = req.body;

    try {
        await db.exec('BEGIN TRANSACTION');

        try {
            // Get Source Product
            const sourceProduct = await db.get('SELECT * FROM products WHERE id = ?', [sourceProductId]);
            if (!sourceProduct) throw new Error('Source product not found');

            if (sourceProduct.stock < quantity) throw new Error('Insufficient stock');

            // 1. Deduct from Source
            await db.run(
                'UPDATE products SET stock = stock - ? WHERE id = ?',
                [quantity, sourceProductId]
            );

            // 2. Find or Create Target Product
            const targetShopId = sourceProduct.shop_id === 'shop-1' ? 'shop-2' : 'shop-1';

            const targetProduct = await db.get(
                'SELECT * FROM products WHERE shop_id = ? AND (sku = ? OR name = ?)',
                [targetShopId, sourceProduct.sku || '', sourceProduct.name]
            );

            if (targetProduct) {
                // Update Existing
                await db.run(
                    'UPDATE products SET stock = stock + ? WHERE id = ?',
                    [quantity, targetProduct.id]
                );
            } else {
                // Create New
                const newId = generateId();
                const createdAt = Date.now();
                await db.run(
                    'INSERT INTO products (id, name, sku, purchase_price, sale_price, stock, category, shop_id, supplier_id, supplier_name, size, unit, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    [newId, sourceProduct.name, sourceProduct.sku, sourceProduct.purchase_price, sourceProduct.sale_price, quantity, sourceProduct.category, targetShopId, sourceProduct.supplier_id, sourceProduct.supplier_name, sourceProduct.size, sourceProduct.unit, createdAt]
                );
            }

            await db.exec('COMMIT');
            res.json({ message: 'Transfer successful' });
        } catch (error) {
            await db.exec('ROLLBACK');
            throw error;
        }
    } catch (error) {
        console.error('Error transferring stock:', error);
        res.status(500).json({ error: error.message });
    }
});

// --- CUSTOMERS API ---

app.get('/api/customers', async (req, res) => {
    try {
        const rows = await db.all('SELECT * FROM customers ORDER BY created_at DESC');
        const customers = rows.map(c => ({
            id: c.id,
            name: c.name,
            phone: c.phone,
            address: c.address,
            balance: Number(c.balance),
            totalPurchased: Number(c.total_purchased),
            totalPaid: Number(c.total_paid),
            createdAt: Number(c.created_at)
        }));
        res.json(customers);
    } catch (error) {
        console.error('Error fetching customers:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/customers', async (req, res) => {
    const { name, phone, address } = req.body;
    const id = generateId();
    const createdAt = Date.now();
    try {
        await db.run(
            'INSERT INTO customers (id, name, phone, address, balance, total_purchased, total_paid, created_at) VALUES (?, ?, ?, ?, 0, 0, 0, ?)',
            [id, name, phone, address, createdAt]
        );
        res.json({ id, name, phone, address, balance: 0, totalPurchased: 0, totalPaid: 0, createdAt });
    } catch (error) {
        console.error('Error adding customer:', error);
        res.status(500).json({ error: error.message });
    }
});

app.put('/api/customers/:id', async (req, res) => {
    const { name, phone, address } = req.body;
    try {
        await db.run('UPDATE customers SET name=?, phone=?, address=? WHERE id=?', [name, phone, address, req.params.id]);
        res.json({ message: 'Customer updated' });
    } catch (error) {
        console.error('Error updating customer:', error);
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/customers/:id', async (req, res) => {
    try {
        await db.run('DELETE FROM customers WHERE id=?', [req.params.id]);
        res.json({ message: 'Customer deleted' });
    } catch (error) {
        console.error('Error deleting customer:', error);
        res.status(500).json({ error: error.message });
    }
});

// --- SALES API ---

app.get('/api/sales', async (req, res) => {
    try {
        const sales = await db.all('SELECT * FROM sales ORDER BY created_at DESC');
        const items = await db.all('SELECT * FROM sale_items');

        const fullSales = sales.map(s => ({
            id: s.id,
            type: s.type,
            total: Number(s.total),
            createdAt: Number(s.created_at),
            customerId: s.customer_id,
            customerName: s.customer_name,
            customerPhone: s.customer_phone,
            customerAddress: s.customer_address,
            paymentType: s.payment_type,
            amountPaid: Number(s.amount_paid),
            remainingBalance: Number(s.remaining_balance),
            dueDate: s.due_date ? Number(s.due_date) : undefined,
            shopId: s.shop_id,
            items: items.filter(i => i.sale_id === s.id).map(i => ({
                id: i.product_id,
                name: i.name,
                sku: i.sku,
                quantity: i.quantity,
                salePrice: Number(i.sale_price),
                total: Number(i.total),
                unit: i.unit,
                size: i.size
            }))
        }));
        res.json(fullSales);
    } catch (error) {
        console.error('Error fetching sales:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/sales', async (req, res) => {
    const { items, total, customerDetails, shopId, type } = req.body;
    const createdAt = Date.now();

    try {
        await db.exec('BEGIN TRANSACTION');

        try {
            // Generate sequential invoice ID
            const result = await db.get("SELECT MAX(CAST(id AS INTEGER)) as maxId FROM sales WHERE id GLOB '[0-9]*'");
            const nextId = (result?.maxId || 0) + 1;
            const saleId = String(nextId);

            // Insert sale
            await db.run(
                `INSERT INTO sales (
                    id, type, total, created_at, customer_id, customer_name, customer_phone, customer_address, 
                    payment_type, amount_paid, remaining_balance, due_date, shop_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                [
                    saleId, type, total, createdAt, customerDetails.customerId, customerDetails.customerName,
                    customerDetails.customerPhone, customerDetails.customerAddress, customerDetails.paymentType,
                    customerDetails.amountPaid, customerDetails.remainingBalance, customerDetails.dueDate, shopId
                ]
            );

            // Insert sale items and update product stock
            for (const item of items) {
                const itemTotal = item.salePrice * item.quantity;
                await db.run(
                    'INSERT INTO sale_items (sale_id, product_id, name, sku, quantity, sale_price, total, unit, size) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    [saleId, item.id, item.name, item.sku, item.quantity, item.salePrice, itemTotal, item.unit, item.size]
                );

                if (type === 'Return') {
                    await db.run('UPDATE products SET stock = stock + ? WHERE id = ?', [item.quantity, item.id]);
                } else {
                    await db.run('UPDATE products SET stock = stock - ? WHERE id = ?', [item.quantity, item.id]);
                }
            }

            // Update customer if provided
            if (customerDetails.customerId) {
                if (type === 'Sale') {
                    await db.run(
                        'UPDATE customers SET total_purchased = total_purchased + ?, total_paid = total_paid + ?, balance = balance + ? WHERE id = ?',
                        [total, customerDetails.amountPaid, customerDetails.remainingBalance, customerDetails.customerId]
                    );
                } else {
                    await db.run(
                        'UPDATE customers SET balance = balance - ? WHERE id = ?',
                        [customerDetails.remainingBalance, customerDetails.customerId]
                    );
                }
            }

            await db.exec('COMMIT');
            res.status(201).json({ id: saleId, message: 'Sale recorded' });
        } catch (error) {
            await db.exec('ROLLBACK');
            throw error;
        }
    } catch (error) {
        console.error('Error creating sale:', error);
        res.status(500).json({ error: error.message });
    }
});

// --- PAYMENTS API ---

app.get('/api/payments', async (req, res) => {
    try {
        const rows = await db.all('SELECT * FROM payments ORDER BY created_at DESC');
        const payments = rows.map(p => ({
            id: p.id,
            customerId: p.customer_id,
            saleId: p.sale_id,
            amount: Number(p.amount),
            date: Number(p.created_at),
            method: p.method,
            note: p.note
        }));
        res.json(payments);
    } catch (error) {
        console.error('Error fetching payments:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/payments', async (req, res) => {
    const { customerId, amount, method, note, saleId } = req.body;
    const id = generateId();
    const date = Date.now();

    try {
        await db.exec('BEGIN TRANSACTION');

        try {
            // Insert payment
            await db.run(
                'INSERT INTO payments (id, customer_id, sale_id, amount, created_at, method, note) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [id, customerId, saleId, amount, date, method, note]
            );

            // Update customer balance
            await db.run(
                'UPDATE customers SET balance = balance - ?, total_paid = total_paid + ? WHERE id = ?',
                [amount, amount, customerId]
            );

            // Update sale if provided
            if (saleId) {
                await db.run(
                    'UPDATE sales SET amount_paid = amount_paid + ?, remaining_balance = remaining_balance - ? WHERE id = ?',
                    [amount, amount, saleId]
                );

                // Check remaining balance and update payment type if full
                const sale = await db.get('SELECT remaining_balance FROM sales WHERE id = ?', [saleId]);
                if (sale && sale.remaining_balance <= 0) {
                    await db.run('UPDATE sales SET payment_type = ? WHERE id = ?', ['Full', saleId]);
                }
            }

            await db.exec('COMMIT');
            res.json({ id, message: 'Payment recorded' });
        } catch (error) {
            await db.exec('ROLLBACK');
            throw error;
        }
    } catch (error) {
        console.error('Error creating payment:', error);
        res.status(500).json({ error: error.message });
    }
});

// --- EXPENSES API ---

app.get('/api/expenses', async (req, res) => {
    try {
        const rows = await db.all('SELECT * FROM expenses ORDER BY created_at DESC');
        const expenses = rows.map(e => ({
            ...e,
            amount: Number(e.amount),
            date: Number(e.created_at),
            shopId: e.shop_id
        }));
        res.json(expenses);
    } catch (error) {
        console.error('Error fetching expenses:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/expenses', async (req, res) => {
    const { description, amount, category, shopId } = req.body;
    const id = generateId();
    const date = Date.now();
    try {
        await db.run(
            'INSERT INTO expenses (id, description, amount, category, created_at, shop_id) VALUES (?, ?, ?, ?, ?, ?)',
            [id, description, amount, category, date, shopId]
        );
        res.json({ id, description, amount, category, date, shopId });
    } catch (error) {
        console.error('Error adding expense:', error);
        res.status(500).json({ error: error.message });
    }
});

// --- SUPPLIERS API ---

app.get('/api/suppliers', async (req, res) => {
    try {
        const rows = await db.all('SELECT * FROM suppliers ORDER BY created_at DESC');
        const suppliers = rows.map(s => ({
            id: s.id,
            name: s.name,
            contact: s.contact,
            alternatePhone: s.alternate_phone,
            shopName: s.shop_name,
            address: s.address,
            notes: s.notes,
            balance: Number(s.balance),
            totalPurchased: Number(s.total_purchased),
            totalPaid: Number(s.total_paid),
            createdAt: Number(s.created_at),
            nextPaymentDate: s.next_payment_date ? Number(s.next_payment_date) : undefined
        }));
        res.json(suppliers);
    } catch (error) {
        console.error('Error fetching suppliers:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/suppliers', async (req, res) => {
    const { name, contact, alternatePhone, shopName, address, notes } = req.body;
    const id = generateId();
    const createdAt = Date.now();
    try {
        await db.run(
            'INSERT INTO suppliers (id, name, contact, alternate_phone, shop_name, address, notes, balance, total_purchased, total_paid, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?)',
            [id, name, contact, alternatePhone, shopName, address, notes, createdAt]
        );
        res.json({ id, ...req.body, createdAt });
    } catch (error) {
        console.error('Error adding supplier:', error);
        res.status(500).json({ error: error.message });
    }
});

app.put('/api/suppliers/:id', async (req, res) => {
    const { name, contact, alternatePhone, shopName, address, notes } = req.body;
    try {
        await db.run(
            'UPDATE suppliers SET name=?, contact=?, alternate_phone=?, shop_name=?, address=?, notes=? WHERE id=?',
            [name, contact, alternatePhone, shopName, address, notes, req.params.id]
        );
        res.json({ message: 'Supplier updated' });
    } catch (error) {
        console.error('Error updating supplier:', error);
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/suppliers/:id', async (req, res) => {
    try {
        await db.run('DELETE FROM suppliers WHERE id=?', [req.params.id]);
        res.json({ message: 'Supplier deleted' });
    } catch (error) {
        console.error('Error deleting supplier:', error);
        res.status(500).json({ error: error.message });
    }
});

// --- PURCHASES API ---

app.get('/api/purchases', async (req, res) => {
    try {
        const purchases = await db.all('SELECT * FROM purchases ORDER BY created_at DESC');
        const items = await db.all('SELECT * FROM purchase_items');

        const fullPurchases = purchases.map(p => ({
            id: p.id,
            supplierId: p.supplier_id,
            supplierName: p.supplier_name,
            totalAmount: Number(p.total_amount),
            paidAmount: Number(p.paid_amount),
            remainingAmount: Number(p.remaining_amount),
            date: Number(p.created_at),
            shopId: p.shop_id,
            items: items.filter(i => i.purchase_id === p.id).map(i => ({
                productId: i.product_id,
                name: i.name,
                quantity: i.quantity,
                costPrice: Number(i.cost_price),
                total: Number(i.total),
                unit: i.unit,
                size: i.size
            }))
        }));
        res.json(fullPurchases);
    } catch (error) {
        console.error('Error fetching purchases:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/purchases', async (req, res) => {
    const { supplierId, items, paidAmount, shopId, dueDate } = req.body;
    const purchaseId = generateId();
    const date = Date.now();

    // Normalize numbers and robustly compute totals
    const safePaidAmount = Number(paidAmount || 0);

    if (!Array.isArray(items) || items.length === 0) {
        return res.status(400).json({ error: 'items array is required' });
    }

    // Compute total amount defensively - prefer explicit `total` but fall back to costPrice * quantity
    const totalAmount = items.reduce((sum, item) => {
        const q = Number(item.quantity || 0);
        const cp = Number(item.costPrice || item.cost_price || 0);
        const itTotal = Number(item.total !== undefined ? item.total : q * cp) || (q * cp);
        return sum + itTotal;
    }, 0);

    const remainingAmount = totalAmount - safePaidAmount;

    // Basic validation
    if (!supplierId) {
        return res.status(400).json({ error: 'supplierId is required' });
    }

    // Helper wrappers for better debugging around DB calls
    const recentDBCalls = [];

    const runSQL = async (sql, params = []) => {
        console.log('[DB RUN]', sql, params);
        recentDBCalls.push({ type: 'run', sql, params });
        if (recentDBCalls.length > 50) recentDBCalls.shift();
        try {
            return await db.run(sql, params);
        } catch (err) {
            console.error('[DB RUN ERROR]', sql, params, err && (err.stack || err.message));
            recentDBCalls.push({ type: 'error', sql, params, error: err && (err.stack || err.message) });
            throw err;
        }
    };

    const getSQL = async (sql, params = []) => {
        console.log('[DB GET]', sql, params);
        recentDBCalls.push({ type: 'get', sql, params });
        if (recentDBCalls.length > 50) recentDBCalls.shift();
        try {
            return await db.get(sql, params);
        } catch (err) {
            console.error('[DB GET ERROR]', sql, params, err && (err.stack || err.message));
            recentDBCalls.push({ type: 'error', sql, params, error: err && (err.stack || err.message) });
            throw err;
        }
    };

    try {
        await db.exec('BEGIN TRANSACTION');

        try {
            // 1. Get supplier name
            const supplier = await getSQL('SELECT name FROM suppliers WHERE id = ?', [supplierId]);
            if (!supplier) throw new Error('Supplier not found');
            
            // Insert purchase
            await runSQL(
                'INSERT INTO purchases (id, supplier_id, supplier_name, total_amount, paid_amount, remaining_amount, created_at, shop_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                [purchaseId, supplierId, supplier.name, totalAmount, paidAmount, remainingAmount, date, shopId]
            );

            // 2. Process each item with per-item validation and error context
            for (let i = 0; i < items.length; i++) {
                const item = items[i];

                // Validate item shape
                if (!item || typeof item !== 'object') {
                    throw new Error(`Invalid purchase item at index ${i}: not an object`);
                }

                const productId = item.productId || item.product_id;
                const name = item.name || item.n;
                const quantity = Number(item.quantity || 0);
                const costPrice = Number(item.costPrice || item.cost_price || 0);
                const computedTotal = Number(item.total !== undefined ? item.total : quantity * costPrice) || (quantity * costPrice);

                if (!productId || typeof productId !== 'string') {
                    throw new Error(`Missing or invalid productId for item at index ${i}: ${JSON.stringify(item)}`);
                }

                if (!name) {
                    console.warn(`[PURCHASE] item ${i} missing name`, item);
                }

                if (quantity <= 0) {
                    console.warn(`[PURCHASE] item ${i} has non-positive quantity`, item);
                }

                // Insert purchase item with explicit sanitized parameters
                try {
                    await runSQL(
                        'INSERT INTO purchase_items (purchase_id, product_id, name, quantity, cost_price, total, unit, size) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                        [purchaseId, productId, name, quantity, costPrice, computedTotal, item.unit, item.size]
                    );

                    // Update product stock and supplier info
                    await runSQL(
                        'UPDATE products SET stock = stock + ?, purchase_price = ?, supplier_id = ?, supplier_name = ? WHERE id = ?',
                        [quantity, costPrice, supplierId, supplier.name, productId]
                    );
                } catch (err) {
                    // Augment error with index and item for easier debugging
                    const msg = `Error processing purchase item at index ${i} (productId=${productId}): ${err && (err.message || err)} `;
                    console.error(msg, err && (err.stack || err.message));
                    // Add to recentDBCalls for visibility and rethrow
                    recentDBCalls.push({ type: 'itemError', index: i, item, error: err && (err.stack || err.message) });
                    throw new Error(msg);
                }
            }

            // 3. Update supplier totals
            await runSQL(
                'UPDATE suppliers SET total_purchased = total_purchased + ?, balance = balance + ? WHERE id = ?',
                [totalAmount, totalAmount, supplierId]
            );

            // 4. Update due date if provided
            if (dueDate) {
                await runSQL('UPDATE suppliers SET next_payment_date = ? WHERE id = ?', [dueDate, supplierId]);
            }

            // 5. Process immediate payment if any
            if (paidAmount > 0) {
                const paymentId = generateId();
                await runSQL(
                    'INSERT INTO supplier_payments (id, supplier_id, amount, created_at, method, note) VALUES (?, ?, ?, ?, ?, ?)',
                    [paymentId, supplierId, paidAmount, date, 'Cash', `Immediate payment for Purchase #${purchaseId}`]
                );

                await runSQL(
                    'UPDATE suppliers SET balance = balance - ?, total_paid = total_paid + ? WHERE id = ?',
                    [paidAmount, paidAmount, supplierId]
                );
            }

            await db.exec('COMMIT');
            
            res.status(201).json({ 
                id: purchaseId, 
                message: 'Purchase recorded',
                totalAmount,
                remainingAmount 
            });
        } catch (error) {
            await db.exec('ROLLBACK');
            throw error;
        }
    } catch (error) {
        console.error('Error creating purchase:', error && (error.stack || error.message));
        // Return stack + recent DB calls for faster debugging in dev
        res.status(500).json({ error: error.stack || error.message, recentDBCalls });
    }
});

// --- SUPPLIER PAYMENTS API ---

app.get('/api/supplier-payments', async (req, res) => {
    try {
        const rows = await db.all('SELECT * FROM supplier_payments ORDER BY created_at DESC');
        const payments = rows.map(p => ({
            id: p.id,
            supplierId: p.supplier_id,
            amount: Number(p.amount),
            date: Number(p.created_at),
            method: p.method,
            note: p.note
        }));
        res.json(payments);
    } catch (error) {
        console.error('Error fetching supplier payments:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/supplier-payments', async (req, res) => {
    const { supplierId, amount, method, note } = req.body;
    const id = generateId();
    const date = Date.now();

    try {
        await db.exec('BEGIN TRANSACTION');
        
        try {
            await db.run(
                'INSERT INTO supplier_payments (id, supplier_id, amount, created_at, method, note) VALUES (?, ?, ?, ?, ?, ?)',
                [id, supplierId, amount, date, method, note]
            );

            await db.run(
                'UPDATE suppliers SET balance = balance - ?, total_paid = total_paid + ? WHERE id = ?',
                [amount, amount, supplierId]
            );

            await db.exec('COMMIT');
            res.json({ id, message: 'Supplier payment recorded' });
        } catch (error) {
            await db.exec('ROLLBACK');
            throw error;
        }
    } catch (error) {
        console.error('Error creating supplier payment:', error);
        res.status(500).json({ error: error.message });
    }
});

// --- RESET API ---
app.post('/api/reset', async (req, res) => {
    try {
        await db.exec('PRAGMA foreign_keys = OFF');

        await db.run('DELETE FROM sale_items');
        await db.run('DELETE FROM sales');
        await db.run('DELETE FROM payments');
        await db.run('DELETE FROM expenses');
        await db.run('DELETE FROM supplier_payments');
        await db.run('DELETE FROM purchase_items');
        await db.run('DELETE FROM purchases');
        await db.run('DELETE FROM customers');
        await db.run('DELETE FROM suppliers');
        await db.run('DELETE FROM products');

        await db.exec('PRAGMA foreign_keys = ON');

        res.json({ message: 'Database reset successful' });
    } catch (error) {
        console.error('Error resetting database:', error);
        res.status(500).json({ error: 'Failed to reset database' });
    }
});

// --- ERROR HANDLING ---
app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    res.status(500).json({ error: 'Internal server error' });
});

// 404 Handler
app.use((req, res) => {
    res.status(404).json({ error: 'Not found' });
});

// Test endpoint
app.get('/api/test', (req, res) => {
    res.json({ message: 'API is working', timestamp: new Date().toISOString() });
});

// Diagnostic DB info - returns versions and types of DB methods
app.get('/api/db-info', (req, res) => {
    try {
        const sqlite3Pkg = require('sqlite3/package.json');
        const sqlitePkg = require('sqlite/package.json');
        res.json({ 
            node: process.version,
            sqlite3: sqlite3Pkg.version,
            sqlite: sqlitePkg.version,
            dbRunType: typeof db.run,
            dbGetType: typeof db.get,
            dbExecType: typeof db.exec
        });
    } catch (err) {
        res.status(500).json({ error: err && (err.message || err) });
    }
});

// Health check endpoint
app.get('/api/health', async (req, res) => {
    try {
        const result = await db.get('SELECT 1 as test');
        res.json({ 
            status: 'healthy', 
            database: 'connected',
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(500).json({ 
            status: 'unhealthy', 
            database: 'disconnected',
            error: error.message 
        });
    }
});

// Start server after database is initialized
initDb().then(() => {
    console.log('Database initialized successfully');

    const startServer = (port) => {
        const server = app.listen(port, () => {
            console.log(`✓ Server running on port ${port}`);
            console.log(`✓ Database file: ./aura_inventory.db`);
            console.log(`✓ Test endpoint: http://localhost:${port}/api/test`);
            console.log(`✓ Health check: http://localhost:${port}/api/health`);
        });

        server.on('error', (err) => {
            if (err.code === 'EADDRINUSE') {
                console.log(`Port ${port} is busy, trying ${port + 1}...`);
                startServer(port + 1);
            } else {
                console.error('Server error:', err);
                process.exit(1);
            }
        });
    };

    // Start with port 5000, will increment if busy
    startServer(PORT);
}).catch(err => {
    console.error('Failed to initialize database:', err);
    process.exit(1);
});