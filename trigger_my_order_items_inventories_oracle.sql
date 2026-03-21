SET SERVEROUTPUT ON;

BEGIN
    EXECUTE IMMEDIATE 'DROP TRIGGER trg_my_order_items_inventory';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -4080 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP PROCEDURE sp_manage_my_inventory';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -4043 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE my_order_items CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE my_inventories CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
/

CREATE TABLE my_inventories (
    product_id        NUMBER NOT NULL,
    warehouse_id      NUMBER NOT NULL,
    quantity_on_hand  NUMBER NOT NULL,
    CONSTRAINT pk_my_inventories PRIMARY KEY (product_id, warehouse_id),
    CONSTRAINT ck_my_inventories_qty CHECK (quantity_on_hand >= 0)
);

CREATE TABLE my_order_items (
    order_id       NUMBER NOT NULL,
    line_item_id   NUMBER NOT NULL,
    product_id     NUMBER NOT NULL,
    quantity       NUMBER NOT NULL,
    unit_price     NUMBER(12,2),
    CONSTRAINT pk_my_order_items PRIMARY KEY (order_id, line_item_id),
    CONSTRAINT ck_my_order_items_qty CHECK (quantity > 0),
    CONSTRAINT ck_my_order_items_price CHECK (unit_price IS NULL OR unit_price >= 0)
);

CREATE OR REPLACE PROCEDURE sp_manage_my_inventory (
    p_product_id IN my_inventories.product_id%TYPE,
    p_quantity   IN my_inventories.quantity_on_hand%TYPE,
    p_action     IN VARCHAR2
)
AS
    v_warehouse_id   my_inventories.warehouse_id%TYPE;
    v_previous_qty   my_inventories.quantity_on_hand%TYPE;
    v_current_qty    my_inventories.quantity_on_hand%TYPE;
BEGIN
    IF p_quantity IS NULL OR p_quantity <= 0 THEN
        RETURN;
    END IF;

    IF p_action = 'OUT' THEN
        BEGIN
            SELECT warehouse_id, quantity_on_hand
            INTO v_warehouse_id, v_previous_qty
            FROM (
                SELECT warehouse_id, quantity_on_hand
                FROM my_inventories
                WHERE product_id = p_product_id
                  AND quantity_on_hand >= p_quantity
                ORDER BY warehouse_id
            )
            WHERE ROWNUM = 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                BEGIN
                    SELECT warehouse_id, quantity_on_hand
                    INTO v_warehouse_id, v_previous_qty
                    FROM (
                        SELECT warehouse_id, quantity_on_hand
                        FROM my_inventories
                        WHERE product_id = p_product_id
                        ORDER BY quantity_on_hand DESC, warehouse_id DESC
                    )
                    WHERE ROWNUM = 1;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_warehouse_id := 0;
                        v_previous_qty := 0;
                END;

                RAISE_APPLICATION_ERROR(
                    -20001,
                    'Across all warehouses in the business, there are not ' ||
                    p_quantity || ' items of product code ' || p_product_id ||
                    ' in stock. The warehouse with the highest stock is warehouse number ' ||
                    v_warehouse_id || ', which has ' || v_previous_qty || ' items.'
                );
        END;

        UPDATE my_inventories
        SET quantity_on_hand = quantity_on_hand - p_quantity
        WHERE product_id = p_product_id
          AND warehouse_id = v_warehouse_id;

        SELECT quantity_on_hand
        INTO v_current_qty
        FROM my_inventories
        WHERE product_id = p_product_id
          AND warehouse_id = v_warehouse_id;

        DBMS_OUTPUT.PUT_LINE(
            'From the warehouse with id ' || v_warehouse_id ||
            ', where there were ' || v_previous_qty ||
            ' units of product_id ' || p_product_id ||
            ', ' || p_quantity ||
            ' units were deducted and ' || v_current_qty ||
            ' remain available.'
        );
    ELSIF p_action = 'IN' THEN
        BEGIN
            SELECT warehouse_id, quantity_on_hand
            INTO v_warehouse_id, v_previous_qty
            FROM (
                SELECT warehouse_id, quantity_on_hand
                FROM my_inventories
                WHERE product_id = p_product_id
                ORDER BY warehouse_id DESC
            )
            WHERE ROWNUM = 1;

            UPDATE my_inventories
            SET quantity_on_hand = quantity_on_hand + p_quantity
            WHERE product_id = p_product_id
              AND warehouse_id = v_warehouse_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                -- If the product has no inventory row yet, recreate it in warehouse 1.
                v_warehouse_id := 1;
                v_previous_qty := 0;

                INSERT INTO my_inventories (product_id, warehouse_id, quantity_on_hand)
                VALUES (p_product_id, v_warehouse_id, p_quantity);
        END;

        SELECT quantity_on_hand
        INTO v_current_qty
        FROM my_inventories
        WHERE product_id = p_product_id
          AND warehouse_id = v_warehouse_id;

        DBMS_OUTPUT.PUT_LINE(
            'From the warehouse with id ' || v_warehouse_id ||
            ', where there were ' || v_previous_qty ||
            ' units of product_id ' || p_product_id ||
            ', ' || p_quantity ||
            ' units were returned and ' || v_current_qty ||
            ' remain available.'
        );
    ELSE
        RAISE_APPLICATION_ERROR(-20002, 'Unsupported inventory action: ' || p_action);
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_my_order_items_inventory
AFTER INSERT OR DELETE OR UPDATE OF quantity ON my_order_items
FOR EACH ROW
DECLARE
    v_quantity_delta  NUMBER;
BEGIN
    IF INSERTING THEN
        sp_manage_my_inventory(:NEW.product_id, :NEW.quantity, 'OUT');
    ELSIF DELETING THEN
        sp_manage_my_inventory(:OLD.product_id, :OLD.quantity, 'IN');
    ELSIF UPDATING('quantity') THEN
        v_quantity_delta := :NEW.quantity - :OLD.quantity;

        IF v_quantity_delta > 0 THEN
            sp_manage_my_inventory(:NEW.product_id, v_quantity_delta, 'OUT');
        ELSIF v_quantity_delta < 0 THEN
            sp_manage_my_inventory(:NEW.product_id, ABS(v_quantity_delta), 'IN');
        END IF;
    END IF;
END;
/

INSERT INTO my_inventories (product_id, warehouse_id, quantity_on_hand)
VALUES (301, 1, 20);

INSERT INTO my_inventories (product_id, warehouse_id, quantity_on_hand)
VALUES (301, 2, 50);

INSERT INTO my_inventories (product_id, warehouse_id, quantity_on_hand)
VALUES (301, 5, 12);

INSERT INTO my_inventories (product_id, warehouse_id, quantity_on_hand)
VALUES (302, 1, 4);

INSERT INTO my_inventories (product_id, warehouse_id, quantity_on_hand)
VALUES (302, 3, 8);

INSERT INTO my_inventories (product_id, warehouse_id, quantity_on_hand)
VALUES (302, 7, 25);

INSERT INTO my_inventories (product_id, warehouse_id, quantity_on_hand)
VALUES (303, 2, 30);

INSERT INTO my_inventories (product_id, warehouse_id, quantity_on_hand)
VALUES (303, 4, 10);

COMMIT;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Objects MY_ORDER_ITEMS and MY_INVENTORIES created successfully.');
    DBMS_OUTPUT.PUT_LINE('Suggested tests:');
    DBMS_OUTPUT.PUT_LINE('  INSERT INTO my_order_items (order_id, line_item_id, product_id, quantity, unit_price) VALUES (9001, 1, 301, 10, 1200);');
    DBMS_OUTPUT.PUT_LINE('  UPDATE my_order_items SET quantity = 14 WHERE order_id = 9001 AND line_item_id = 1;');
    DBMS_OUTPUT.PUT_LINE('  UPDATE my_order_items SET quantity = 6 WHERE order_id = 9001 AND line_item_id = 1;');
    DBMS_OUTPUT.PUT_LINE('  DELETE FROM my_order_items WHERE order_id = 9001 AND line_item_id = 1;');
    DBMS_OUTPUT.PUT_LINE('  INSERT INTO my_order_items (order_id, line_item_id, product_id, quantity, unit_price) VALUES (9002, 1, 301, 90, 1200);');
END;
/
