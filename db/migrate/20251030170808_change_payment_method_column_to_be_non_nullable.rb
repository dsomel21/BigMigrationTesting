class ChangePaymentMethodColumnToBeNonNullable < ActiveRecord::Migration[8.0]
  def change
    change_column :transactions, :payment_method, :string, null: false
  end
end
