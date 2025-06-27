async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    await query.answer()

    try:
        # --- 1. ОСНОВНЫЕ МЕНЮ ---
        if data == "main_menu": await main_menu(update, context)
        elif data == "close": await close_menu(update, context)
        elif data == "finance_menu": await finance_menu(update, context)
        elif data == "suppliers_menu": await suppliers_menu(update, context)
        elif data == "debts_menu": await debts_menu(update, context)
        elif data == "admin_panel": await admin_panel(update, context)
        elif data == "staff_management": await staff_management_menu(update, context)
        elif data == "stock_safe_menu": await stock_safe_menu(update, context)
        elif data == "staff_menu": await staff_menu(update, context)
        
        # --- 2. ПЛАНИРОВАНИЕ ---
        elif data == "planning": await start_planning(update, context)
        elif data.startswith("plan_sup_"): await handle_planning_supplier_choice(update, context)
        elif data.startswith("plan_pay_"): await handle_planning_pay_type(update, context)
        
        # --- 3. ЖУРНАЛ ПРИБЫТИЯ И РЕДАКТИРОВАНИЕ ПЛАНОВ ---
        elif data == "view_suppliers": await show_arrivals_journal(update, context)
        elif data.startswith("toggle_arrival_"): await toggle_arrival_status(update, context)
        elif data.startswith("edit_plan_field_"): await edit_plan_choose_field(update, context)
        elif data.startswith("edit_plan_value_"): await edit_plan_save_value(update, context)
        elif data.startswith("edit_plan_"): await edit_plan_start(update, context)

        # --- 4. РЕДАКТИРОВАНИЕ НАКЛАДНОЙ (НОВОЕ) ---
        elif data.startswith("edit_invoice_start_"):
            # Начинаем процесс редактирования
            row_index = int(data.split('_')[-1])
            
            # --- ИСПРАВЛЕНИЕ ЗДЕСЬ: Правильно инициализируем ВСЕ нужные поля ---
            context.user_data['edit_invoice'] = {
                'row_index': row_index,
                'selected_fields': {}, # Поля, которые пользователь выберет для редактирования
                'new_values': {}       # Словарь для хранения новых введенных значений
            }
            
            all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
            # Проверяем, что индекс не выходит за пределы списка
            if row_index - 2 < len(all_invoices):
                invoice_data = all_invoices[row_index - 2]
                kb = build_edit_invoice_keyboard(invoice_data, {}, row_index)
                await query.message.edit_text("<b>✏️ Редактирование накладной</b>\n\nВыберите галочками поля для изменения и нажмите 'Сохранить'.",
                                              parse_mode=ParseMode.HTML, reply_markup=kb)
            else:
                await query.message.edit_text("❌ Ошибка: не удалось найти накладную для редактирования.")


        elif data.startswith("edit_invoice_toggle_"):
            parts = data.split('_')
            row_index = int(parts[3])
            field = "_".join(parts[4:])
            
            edit_state = context.user_data.get('edit_invoice', {})
            if edit_state.get('row_index') != row_index: return

            if field in edit_state['selected_fields']:
                del edit_state['selected_fields'][field]
            else:
                edit_state['selected_fields'][field] = None
            
            all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
            invoice_data = all_invoices[row_index - 2]
            kb = build_edit_invoice_keyboard(invoice_data, edit_state['selected_fields'], row_index)
            await query.message.edit_text("<b>✏️ Редактирование накладной</b>\n\nВыберите галочками поля для изменения и нажмите 'Сохранить'.",
                                          parse_mode=ParseMode.HTML, reply_markup=kb)

        elif data.startswith("edit_invoice_save_"):
             await ask_for_invoice_edit_value(update, context)

        elif data.startswith("edit_invoice_cancel_"):
            row_index = int(data.split('_')[-1])
            day_invoice_rows = context.user_data.get('day_invoice_rows', [])
            try:
                list_index = day_invoice_rows.index(row_index)
                all_invoices = get_cached_sheet_data(context, SHEET_SUPPLIERS)
                date_str = sdate(pdate(all_invoices[row_index-2][0]))
                query.data = f"view_single_invoice_{date_str}_{list_index}"
                await show_single_invoice(update, context)
            except (ValueError, IndexError):
                await suppliers_menu(update, context)
            context.user_data.pop('edit_invoice', None)
            
        elif data.startswith("invoice_edit_value_"):
            value = data.replace("invoice_edit_value_", "")
            edit_state = context.user_data.get('edit_invoice', {})
            fields_to_edit = edit_state.get('fields_to_edit_list', [])
            current_index = edit_state.get('current_field_index', 0)
            if fields_to_edit and current_index < len(fields_to_edit):
                current_field_key = fields_to_edit[current_index]
                edit_state['new_values'][current_field_key] = value
                edit_state['current_field_index'] += 1
                if current_field_key == 'pay_type' and value == 'Долг' and 'due_date' not in fields_to_edit:
                    fields_to_edit.append('due_date')
            await ask_for_invoice_edit_value(update, context)
            
        elif data.startswith("execute_invoice_edit_"):
            await execute_invoice_edit(update, context)

        # --- 5. ДОБАВЛЕНИЕ НАКЛАДНОЙ ---
        elif data == "add_supplier": await start_supplier(update, context)
        elif data.startswith("add_sup_"): await handle_add_supplier_choice(update, context)
        elif data.startswith("pay_"): await handle_supplier_pay_type(update, context)
        elif data == "skip_comment_supplier": await save_supplier(update, context)

        # --- 6. СДАЧА СМЕНЫ ---
        elif data == "add_report": await start_report(update, context)
        elif data.startswith("seller_"): await handle_report_seller(update, context)
        elif data in ("exp_yes", "exp_no"): await handle_report_expenses_ask(update, context)
        elif data in ("more_yes", "more_no"): await handle_expense_more(update, context)
        elif data == "skip_comment": await save_report(update, context)
        
        # --- 7. ПРОСМОТР ОТЧЕТОВ ---
        elif data == "view_reports_menu": await view_reports_menu(update, context)
        elif data == "report_today": await get_report_today(update, context)
        elif data == "report_yesterday": await get_report_yesterday(update, context)
        elif data.startswith("report_week_"):
            if data == "report_week_current": await get_report_week(update, context)
            else: _, _, start_str, end_str = data.split('_', 3); await show_report(update, context, pdate(start_str), pdate(end_str))
        elif data.startswith("report_month_"):
            if data == "report_month_current": await get_report_month(update, context)
            else: _, _, start_str, end_str = data.split('_', 3); await show_report(update, context, pdate(start_str), pdate(end_str))
        elif data == "report_custom": 
            await get_report_custom(update, context)
        
        # --- 8. ДЕТАЛИЗАЦИЯ ОТЧЕТОВ ---
        elif data == "view_today_invoices": await show_today_invoices(update, context)
        elif data.startswith("choose_date_"): await choose_details_date(update, context)
        elif data.startswith("details_exp_"): await show_expenses_detail(update, context)
        elif data.startswith("details_sup_"): await show_suppliers_detail(update, context)
        elif data.startswith("detail_report_nav_"): await show_detailed_report(update, context)
        elif data.startswith("invoices_list_"): await show_invoices_list(update, context)
        elif data.startswith("view_single_invoice_"): await show_single_invoice(update, context)
        
        # --- 9. ДОЛГИ ---
        elif data.startswith("current_debts_"):
            await show_current_debts(update, context, page=int(data.split('_')[-1]))
        elif data == "upcoming_payments": await show_upcoming_payments(update, context)
        elif data == "close_debt": await view_repayable_debts(update, context)
        elif data == "search_debts": await search_debts_start(update, context)
        elif data.startswith("repay_confirm_"):
            await repay_confirm(update, context, int(data.split('_')[2]))
        elif data.startswith("repay_final_"):
            await repay_final(update, context, int(data.split('_')[2]))
        elif data.startswith("debts_history_"):
            await view_debts_history(update, context, page=int(data.split('_')[-1]))
            
        # --- 10. УПРАВЛЕНИЕ ПЕРСОНАЛОМ (АДМИН) ---
        elif data.startswith("view_salary_"): await show_seller_salary_details(update, context)
        elif data.startswith("confirm_payout_"): await confirm_payout(update, context)
        elif data.startswith("execute_payout_"): await execute_payout(update, context)
        elif data.startswith("salary_history_"): await show_salary_history(update, context)
        elif data == "view_shifts":
            await view_shifts_calendar(update, context)
        elif data == "edit_shifts":
            await edit_shifts_calendar(update, context)
        elif data.startswith("shift_nav_"):
            _, _, year, month = data.split('_')
        # Определяем, в каком режиме мы были
        # Мы можем сохранить это в user_data или просто решить по-умолчанию
        # Для простоты, пусть навигация всегда ведет в режим админа, если он админ
            if str(query.from_user.id) in ADMINS:
                await edit_shifts_calendar(update, context, int(year), int(month))
            else:
                await view_shifts_calendar(update, context, int(year), int(month))
        elif data.startswith("edit_shift_"):
            await edit_single_shift(update, context)
        elif data.startswith("toggle_seller_"):
            await toggle_seller_for_shift(update, context)
        elif data == "save_shift":
            await save_shift_changes(update, context)
        elif data.startswith("view_shift_"):
            await show_shift_details(update, context)
        elif data == "seller_stats":
            await query.answer("📈 Статистика в разработке...", show_alert=True)
        elif data == "seller_stats":
            await show_seller_stats_menu(update, context)
        elif data.startswith("view_seller_stats_"):
            await show_seller_stats(update, context)
    
        # --- 11. СЕЙФ И ОСТАТОК ---
        elif data == "inventory_balance": await inventory_balance(update, context)
        elif data == "safe_balance": await safe_balance(update, context)
        elif data == "safe_history": await safe_history(update, context)
        elif data == "inventory_history": await inventory_history(update, context)
        elif data == "safe_deposit": await start_safe_deposit(update, context)
        elif data == "safe_withdraw": await start_safe_withdraw(update, context)
        elif data == "add_inventory_expense": await start_inventory_expense(update, context)

        # --- 12. ПРОЧЕЕ ---
        elif data == "noop": pass
        else:
            await query.answer("Команда не реализована.", show_alert=True)

    except Exception as e:
        logging.error(f"Ошибка обработки callback: {data}. Ошибка: {e}", exc_info=True)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Произошла критическая ошибка: {e}")
        
