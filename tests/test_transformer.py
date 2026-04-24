import unittest

from integrations.toast_api.transformer import (
    transform_customer_orders,
    transform_menus,
    transform_order_items,
    transform_orders,
    transform_payments,
)


class TransformerTests(unittest.TestCase):
    def test_transform_orders_aggregates_checks_and_skips_voided_orders(self):
        api_orders = [
            {
                "guid": "order-1",
                "businessDate": 20260410,
                "openedDate": "2026-04-10T18:00:00Z",
                "source": "IN_STORE",
                "checks": [
                    {
                        "totalAmount": 25.5,
                        "amount": 20.0,
                        "taxAmount": 2.5,
                        "payments": [{"tipAmount": 3.0}],
                        "appliedDiscounts": [{"discountAmount": 1.25}],
                    },
                    {
                        "voided": True,
                        "totalAmount": 999.0,
                    },
                ],
            },
            {"guid": "order-2", "voided": True},
            {"businessDate": 20260410},
        ]

        rows = transform_orders(api_orders, "loc-1")

        self.assertEqual(1, len(rows))
        self.assertEqual(
            {
                "order_id": "order-1",
                "location_id": "loc-1",
                "business_date": "20260410",
                "order_guid": "order-1",
                "order_time": "2026-04-10T18:00:00Z",
                "order_type": "IN_STORE",
                "total_amount": 25.5,
                "subtotal": 20.0,
                "tax_amount": 2.5,
                "tip_amount": 3.0,
                "discount_amount": 1.25,
            },
            rows[0],
        )

    def test_transform_order_items_skips_voided_rows_and_defaults_missing_fields(self):
        api_orders = [
            {
                "guid": "order-1",
                "businessDate": 20260410,
                "checks": [
                    {
                        "selections": [
                            {
                                "displayName": "Pork Dumplings",
                                "quantity": 2,
                                "preDiscountPrice": 12.5,
                                "price": 10.0,
                                "salesCategory": {"name": "Dumplings"},
                            },
                            {
                                "voided": True,
                                "displayName": "Should Not Appear",
                            },
                            {
                                "displayName": "Noodles",
                                "quantity": None,
                                "preDiscountPrice": None,
                                "price": None,
                            },
                        ]
                    }
                ],
            }
        ]

        rows = transform_order_items(api_orders, "loc-1")

        self.assertEqual(2, len(rows))
        self.assertEqual("Pork Dumplings", rows[0]["item_name"])
        self.assertEqual("Dumplings", rows[0]["category"])
        self.assertEqual(2, rows[0]["quantity"])
        self.assertEqual("", rows[1]["category"])
        self.assertEqual(1, rows[1]["quantity"])
        self.assertEqual(0.0, rows[1]["total_price"])

    def test_transform_payments_flattens_payments(self):
        api_orders = [
            {
                "guid": "order-1",
                "businessDate": 20260410,
                "checks": [
                    {
                        "payments": [
                            {"type": "CARD", "amount": 24.25, "paidDate": "2026-04-10T18:10:00Z"},
                            {"amount": 1.75},
                        ]
                    }
                ],
            }
        ]

        rows = transform_payments(api_orders, "loc-1")

        self.assertEqual(2, len(rows))
        self.assertEqual("CARD", rows[0]["payment_method"])
        self.assertEqual("UNKNOWN", rows[1]["payment_method"])
        self.assertEqual(1.75, rows[1]["amount"])

    def test_transform_customer_orders_only_keeps_checks_with_customer_data(self):
        api_orders = [
            {
                "guid": "order-1",
                "businessDate": 20260410,
                "checks": [
                    {"customer": {"email": "guest@example.com ", "firstName": " Ada ", "lastName": ""}},
                    {"customer": {}},
                ],
            }
        ]

        rows = transform_customer_orders(api_orders, "loc-1")

        self.assertEqual(
            [
                {
                    "order_guid": "order-1",
                    "location_id": "loc-1",
                    "business_date": "20260410",
                    "customer_email": "guest@example.com",
                    "customer_phone": None,
                    "first_name": "Ada",
                    "last_name": None,
                }
            ],
            rows,
        )

    def test_transform_menus_reads_nested_menu_groups(self):
        api_menus = [
            {
                "menus": [
                    {
                        "menuGroups": [
                            {
                                "name": "Dumplings",
                                "menuItems": [
                                    {"name": "Pork Dumplings", "price": 8.5},
                                    {"name": "", "price": 9.0},
                                ],
                            }
                        ]
                    }
                ]
            }
        ]

        rows = transform_menus(api_menus, "loc-1", "20260410")

        self.assertEqual(
            [
                {
                    "location_id": "loc-1",
                    "item_name": "Pork Dumplings",
                    "category": "Dumplings",
                    "current_stock": 0.0,
                    "reorder_level": 0.0,
                    "unit_cost": 8.5,
                    "snapshot_date": "20260410",
                    "status": "good",
                }
            ],
            rows,
        )


if __name__ == "__main__":
    unittest.main()
