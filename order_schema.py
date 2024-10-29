from pydantic import BaseModel, Field
from typing import Optional

class Customer(BaseModel):
    name: str = Field(..., description="Имя клиента")
    address: str = Field(..., description="Адрес клиента")
    phone: Optional[str] = Field(None, description="Телефон клиента")

class Order(BaseModel):
    order_id: str = Field(..., description="Уникальный идентификатор заказа")
    customer: Customer
    amount: float = Field(..., gt=0, description="Сумма заказа")
    status: str = Field(..., description="Статус заказа")

class OrderEvent(BaseModel):
    event_type: str = Field(..., description="создание)")
    order: Order