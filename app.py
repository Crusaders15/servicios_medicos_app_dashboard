import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io
import boto3
from datetime import datetime
from pydantic import BaseModel, Field

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================

class R2Config(BaseModel):
    account_id: str = Field(..., alias="ACCOUNT_ID")
    access_key: str = Field(..., alias="ACCESS_KEY")
    secret_key: str = Field(..., alias="SECRET_KEY")
    endpoint: str = Field(..., alias="R2_ENDPOINT")
    bucket: str = Field(..., alias="R2_BUCKET_NAME")

st.set_page_config(
    page_title="Inteligencia en Salud Chile",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🏥"
)

# ==========================================
# 2. DISEÑO PROFESIONAL
# ==========================================

def set_design():
    st.markdown("""
        <style>
        .stApp {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }
        [data-testid="stSidebar"] {
            background-color: rgba(0,0,0,0.85) !important;
        }
        .metric-card {
            background: rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            padding: 20px;
            border-radius: 10px;
            border: 1px solid rgba(255,255,255,0.2);
        }
        h1, h2, h3 {
            color: white !important;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
        }
        .stTabs [data-baseweb="tab"] {
            background-color: rgba(255,255,255,0.2);
            border-radius: 8px;
            padding: 10px 20px;
        }
        .stTabs [aria-selected="true"] {
            background-color: white;
            color: #667eea;
        }
        </style>
    """, unsafe_allow_html=True)

set_design()

# ==========================================
# 3. SISTEMA DE AUTENTICACIÓN
# ==========================================

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state.password_correct = False
    
    if st.session_state.password_correct:
        return True
    
    st.markdown("<h1 style='text-align: center; color: white;'>🔐 Inteligencia en Salud - Chile</h1>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        pwd_input = st.text_input("Código de Acceso", type="password", key="pwd_input")
        
        if pwd_input:
            secret_pwd = st.secrets.get("GENERAL", {}).get("APP_PASSWORD", "salud2025")
            
            if pwd_input == secret_pwd:
                st.session_state.password_correct = True
                st.rerun()
            else:
                st.error("❌ Acceso Denegado")
    
    st.info("💡 Contraseña por defecto: **salud2025**")
    return False

# ==========================================
# 4. CARGA DE DATOS DESDE R2
# ==========================================

@st.cache_data(ttl=3600)
def load_data_from_r2():
    """Carga el archivo de 2GB desde R2 con optimizaciones"""
    try:
        settings = R2Config(**st.secrets["R2"])
        
        s3 = boto3.client(
            service_name="s3",
            endpoint_url=settings.endpoint,
            aws_access_key_id=settings.access_key,
            aws_secret_access_key=settings.secret_key,
            region_name="auto"
        )
        
        # Descarga el archivo
        with st.spinner('📥 Descargando datos desde R2... (2GB, puede tomar unos minutos)'):
            response = s3.get_object(Bucket=settings.bucket, Key='07OCCompraAgil.csv')
            csv_data = response['Body'].read()
        
        # Lectura optimizada con DuckDB
        with st.spinner('⚡ Procesando 546,855 registros con DuckDB...'):
            con = duckdb.connect(database=':memory:')
            
            # Cargar CSV directo a DuckDB (más rápido que pandas)
            con.execute("""
                CREATE TABLE compras AS 
                SELECT * FROM read_csv_auto(?)
            """, [io.BytesIO(csv_data)])
            
            # Convertir fechas
            con.execute("""
                ALTER TABLE compras 
                ADD COLUMN FechaEnvioOC_parsed DATE;
                
                UPDATE compras 
                SET FechaEnvioOC_parsed = TRY_CAST(FechaEnvioOC AS DATE);
            """)
        
        return con, None
        
    except Exception as e:
        return None, str(e)

# ==========================================
# 5. LISTAS DE REFERENCIA
# ==========================================

REGIONES_CHILE = [
    'Arica y Parinacota', 'Tarapacá', 'Antofagasta', 'Atacama', 'Coquimbo',
    'Valparaíso', 'Metropolitana de Santiago', "Libertador Gral. Bernardo O'Higgins",
    'Maule', 'Ñuble', 'Biobío', 'La Araucanía', 'Los Ríos', 'Los Lagos',
    'Aysén del Gral. Carlos Ibáñez del Campo', 'Magallanes y de la Antártica Chilena'
]

# ==========================================
# 6. DASHBOARD PRINCIPAL
# ==========================================

if check_password():
    st.title("🏥 Inteligencia en Salud - Chile")
    st.markdown("**Dashboard de Análisis de Compras Ágiles en el Sector Salud**")
    
    # Cargar datos
    con, error = load_data_from_r2()
    
    if con is None:
        st.error(f"⚠️ Error al cargar datos desde R2: {error}")
        st.info("📊 El sistema requiere acceso a R2 para funcionar correctamente.")
        st.stop()
    
    # ==========================================
    # SIDEBAR: FILTROS GLOBALES
    # ==========================================
    
    st.sidebar.title("🎯 Filtros Globales")
    
    # Filtro de Solo Salud
    solo_salud = st.sidebar.checkbox("✅ Solo Sector Salud", value=True)
    
    # Fechas
    st.sidebar.subheader("📅 Período de Análisis")
    fecha_inicio = st.sidebar.date_input("Fecha Inicio", value=datetime(2025, 1, 1))
    fecha_fin = st.sidebar.date_input("Fecha Fin", value=datetime(2025, 12, 31))
    
    # Regiones
    st.sidebar.subheader("📍 Regiones")
    
    region_proveedor_options = ['Todas'] + REGIONES_CHILE
    region_proveedor = st.sidebar.selectbox("Región del Proveedor", region_proveedor_options)
    
    region_compra_options = ['Todas'] + REGIONES_CHILE
    region_compra = st.sidebar.selectbox("Región Unidad de Compra", region_compra_options)
    
    # Especialidades (dinámico desde datos)
    especialidades_query = "SELECT DISTINCT ONUProducto FROM compras WHERE ONUProducto IS NOT NULL ORDER BY ONUProducto"
    especialidades_df = con.execute(especialidades_query).df()
    especialidades = ['Todas'] + especialidades_df['ONUProducto'].tolist()
    especialidad_selected = st.sidebar.selectbox("🏥 Especialidad/Servicio", especialidades)
    
    # Búsqueda
    st.sidebar.subheader("🔍 Búsqueda")
    search_term = st.sidebar.text_input("Buscar Proveedor (Nombre o RUT)")
    
    # ==========================================
    # CONSTRUCCIÓN DE QUERY DINÁMICA
    # ==========================================
    
    where_clauses = []
    
    # Filtro de salud
    if solo_salud:
        where_clauses.append("(LOWER(RubroN1) LIKE '%salud%' OR LOWER(RubroN1) LIKE '%médico%')")
    
    # Filtro de fechas
    where_clauses.append(f"FechaEnvioOC_parsed >= DATE '{fecha_inicio}'")
    where_clauses.append(f"FechaEnvioOC_parsed <= DATE '{fecha_fin}'")
    
    # Filtro de regiones
    if region_proveedor != 'Todas':
        where_clauses.append(f"RegionProveedor = '{region_proveedor}'")
    
    if region_compra != 'Todas':
        where_clauses.append(f"RegionUnidadCompra = '{region_compra}'")
    
    # Filtro de especialidad
    if especialidad_selected != 'Todas':
        where_clauses.append(f"ONUProducto = '{especialidad_selected}'")
    
    # Filtro de búsqueda
    if search_term:
        where_clauses.append(f"(LOWER(Proveedor) LIKE '%{search_term.lower()}%' OR ProveedorRUT LIKE '%{search_term}%')")
    
    where_sql = " AND ".join(where_clauses)
    
    # ==========================================
    # MÉTRICAS PRINCIPALES (KPIs)
    # ==========================================
    
    with st.spinner('📊 Calculando métricas...'):
        # Total adjudicadores únicos
        kpi_query = f"""
            SELECT 
                COUNT(DISTINCT ProveedorRUT) as total_adjudicadores,
                COUNT(*) as total_ordenes,
                SUM(TRY_CAST(MontoTotalOC_CLP AS DOUBLE)) as monto_total
            FROM compras
            WHERE {where_sql}
        """
        kpis = con.execute(kpi_query).df().iloc[0]
    
    # Mostrar KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "👥 Total Adjudicadores",
            f"{int(kpis['total_adjudicadores']):,}",
            help="Proveedores únicos (por RUT)"
        )
    
    with col2:
        st.metric(
            "📋 Órdenes de Compra",
            f"{int(kpis['total_ordenes']):,}",
            help="Total de OC generadas"
        )
    
    with col3:
        monto_miles_millones = kpis['monto_total'] / 1_000_000_000 if kpis['monto_total'] else 0
        st.metric(
            "💰 Monto Total",
            f"${monto_miles_millones:,.1f}B CLP",
            help="Suma total de MontoTotalOC_CLP"
        )
    
    with col4:
        regiones_activas_query = f"""
            SELECT COUNT(DISTINCT RegionProveedor) as regiones
            FROM compras
            WHERE {where_sql}
        """
        regiones_activas = con.execute(regiones_activas_query).df().iloc[0]['regiones']
        st.metric(
            "📍 Regiones Activas",
            f"{int(regiones_activas)}",
            help="Regiones con proveedores"
        )
    
    st.markdown("---")
    
    # ==========================================
    # TABS DE CONTENIDO
    # ==========================================
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Resumen Ejecutivo",
        "📍 Análisis Regional",
        "🏥 Por Especialidad",
        "📋 Datos Detallados"
    ])
    
    # ==========================================
    # TAB 1: RESUMEN EJECUTIVO
    # ==========================================
    
    with tab1:
        st.header("📊 Resumen Ejecutivo")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Top 10 Especialidades Más Contratadas")
            
            top_especialidades_query = f"""
                SELECT 
                    ONUProducto as especialidad,
                    COUNT(*) as cantidad
                FROM compras
                WHERE {where_sql} AND ONUProducto IS NOT NULL
                GROUP BY ONUProducto
                ORDER BY cantidad DESC
                LIMIT 10
            """
            df_esp = con.execute(top_especialidades_query).df()
            
            if not df_esp.empty:
                fig1 = px.bar(
                    df_esp,
                    x='cantidad',
                    y='especialidad',
                    orientation='h',
                    title='Cantidad de Órdenes por Especialidad',
                    color='cantidad',
                    color_continuous_scale='Blues'
                )
                fig1.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='white',
                    yaxis={'categoryorder':'total ascending'}
                )
                st.plotly_chart(fig1, use_container_width=True)
            else:
                st.info("No hay datos para mostrar con los filtros seleccionados")
        
        with col2:
            st.subheader("Distribución de Montos por Región")
            
            monto_region_query = f"""
                SELECT 
                    RegionProveedor as region,
                    SUM(TRY_CAST(MontoTotalOC_CLP AS DOUBLE)) as monto
                FROM compras
                WHERE {where_sql}
                GROUP BY RegionProveedor
                ORDER BY monto DESC
                LIMIT 10
            """
            df_monto = con.execute(monto_region_query).df()
            
            if not df_monto.empty:
                fig2 = px.pie(
                    df_monto,
                    values='monto',
                    names='region',
                    title='Distribución de Montos (Top 10)'
                )
                fig2.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='white'
                )
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("No hay datos para mostrar")
        
        # Tendencia temporal
        st.subheader("📈 Tendencia Temporal de Órdenes")
        
        tendencia_query = f"""
            SELECT 
                DATE_TRUNC('month', FechaEnvioOC_parsed) as mes,
                COUNT(*) as ordenes,
                COUNT(DISTINCT ProveedorRUT) as proveedores
            FROM compras
            WHERE {where_sql}
            GROUP BY DATE_TRUNC('month', FechaEnvioOC_parsed)
            ORDER BY mes
        """
        df_tend = con.execute(tendencia_query).df()
        
        if not df_tend.empty:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=df_tend['mes'],
                y=df_tend['ordenes'],
                name='Órdenes',
                mode='lines+markers',
                line=dict(color='#00C49F', width=3)
            ))
            fig3.add_trace(go.Scatter(
                x=df_tend['mes'],
                y=df_tend['proveedores'],
                name='Proveedores Únicos',
                mode='lines+markers',
                line=dict(color='#FF8042', width=3),
                yaxis='y2'
            ))
            fig3.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white',
                yaxis=dict(title='Órdenes'),
                yaxis2=dict(title='Proveedores', overlaying='y', side='right')
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No hay datos temporales")
    
    # ==========================================
    # TAB 2: ANÁLISIS REGIONAL
    # ==========================================
    
    with tab2:
        st.header("📍 Análisis Regional Detallado")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Adjudicadores por Región del Proveedor")
            
            adj_region_query = f"""
                SELECT 
                    RegionProveedor as region,
                    COUNT(DISTINCT ProveedorRUT) as adjudicadores,
                    COUNT(*) as ordenes
                FROM compras
                WHERE {where_sql}
                GROUP BY RegionProveedor
                ORDER BY adjudicadores DESC
            """
            df_adj_reg = con.execute(adj_region_query).df()
            
            if not df_adj_reg.empty:
                st.dataframe(
                    df_adj_reg.style.format({
                        'adjudicadores': '{:,}',
                        'ordenes': '{:,}'
                    }),
                    use_container_width=True,
                    height=400
                )
                
                # Gráfico
                fig4 = px.bar(
                    df_adj_reg,
                    x='adjudicadores',
                    y='region',
                    orientation='h',
                    title='Proveedores Únicos por Región',
                    color='adjudicadores',
                    color_continuous_scale='Viridis'
                )
                fig4.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='white',
                    yaxis={'categoryorder':'total ascending'}
                )
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.info("No hay datos")
        
        with col2:
            st.subheader("Compras por Región Unidad Compradora")
            
            compras_region_query = f"""
                SELECT 
                    RegionUnidadCompra as region,
                    COUNT(*) as ordenes,
                    SUM(TRY_CAST(MontoTotalOC_CLP AS DOUBLE)) as monto_total
                FROM compras
                WHERE {where_sql}
                GROUP BY RegionUnidadCompra
                ORDER BY ordenes DESC
            """
            df_comp_reg = con.execute(compras_region_query).df()
            
            if not df_comp_reg.empty:
                df_comp_reg['monto_millones'] = df_comp_reg['monto_total'] / 1_000_000
                
                st.dataframe(
                    df_comp_reg[['region', 'ordenes', 'monto_millones']].style.format({
                        'ordenes': '{:,}',
                        'monto_millones': '${:,.1f}M'
                    }),
                    use_container_width=True,
                    height=400
                )
                
                # Gráfico
                fig5 = px.bar(
                    df_comp_reg,
                    x='ordenes',
                    y='region',
                    orientation='h',
                    title='Órdenes por Región Compradora',
                    color='monto_millones',
                    color_continuous_scale='RdYlGn'
                )
                fig5.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='white',
                    yaxis={'categoryorder':'total ascending'}
                )
                st.plotly_chart(fig5, use_container_width=True)
            else:
                st.info("No hay datos")
    
    # ==========================================
    # TAB 3: POR ESPECIALIDAD
    # ==========================================
    
    with tab3:
        st.header("🏥 Análisis por Especialidad")
        
        especialidades_completo_query = f"""
            SELECT 
                ONUProducto as especialidad,
                COUNT(*) as ordenes,
                COUNT(DISTINCT ProveedorRUT) as proveedores,
                SUM(TRY_CAST(MontoTotalOC_CLP AS DOUBLE)) as monto_total,
                AVG(TRY_CAST(MontoTotalOC_CLP AS DOUBLE)) as monto_promedio
            FROM compras
            WHERE {where_sql} AND ONUProducto IS NOT NULL
            GROUP BY ONUProducto
            ORDER BY ordenes DESC
        """
        df_esp_completo = con.execute(especialidades_completo_query).df()
        
        if not df_esp_completo.empty:
            df_esp_completo['monto_total_millones'] = df_esp_completo['monto_total'] / 1_000_000
            df_esp_completo['monto_promedio_miles'] = df_esp_completo['monto_promedio'] / 1_000
            df_esp_completo['participacion'] = (df_esp_completo['ordenes'] / df_esp_completo['ordenes'].sum() * 100).round(1)
            
            st.dataframe(
                df_esp_completo[[
                    'especialidad', 'ordenes', 'proveedores',
                    'monto_total_millones', 'monto_promedio_miles', 'participacion'
                ]].style.format({
                    'ordenes': '{:,}',
                    'proveedores': '{:,}',
                    'monto_total_millones': '${:,.1f}M',
                    'monto_promedio_miles': '${:,.0f}K',
                    'participacion': '{:.1f}%'
                }),
                use_container_width=True,
                height=600
            )
            
            # Exportar
            csv = df_esp_completo.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Descargar Tabla como CSV",
                data=csv,
                file_name=f"especialidades_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No hay datos de especialidades")
    
    # ==========================================
    # TAB 4: DATOS DETALLADOS
    # ==========================================
    
    with tab4:
        st.header("📋 Vista Detallada de Registros")
        
        # Límite de registros a mostrar
        limit = st.slider("Número de registros a mostrar", 100, 5000, 1000, 100)
        
        detalle_query = f"""
            SELECT 
                codigoOC,
                FechaEnvioOC_parsed as Fecha,
                Proveedor,
                ProveedorRUT,
                RegionProveedor,
                RegionUnidadCompra,
                ONUProducto as Especialidad,
                RubroN1 as Rubro,
                TRY_CAST(MontoTotalOC_CLP AS DOUBLE) as Monto_CLP
            FROM compras
            WHERE {where_sql}
            ORDER BY FechaEnvioOC_parsed DESC
            LIMIT {limit}
        """
        
        with st.spinner(f'Cargando {limit} registros...'):
            df_detalle = con.execute(detalle_query).df()
        
        if not df_detalle.empty:
            st.write(f"**Mostrando {len(df_detalle):,} de {int(kpis['total_ordenes']):,} registros**")
            
            st.dataframe(
                df_detalle.style.format({
                    'Monto_CLP': '${:,.0f}'
                }),
                use_container_width=True,
                height=600
            )
            
            # Exportar datos filtrados
            csv_detalle = df_detalle.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Descargar Datos Filtrados (CSV)",
                data=csv_detalle,
                file_name=f"datos_filtrados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No hay registros para mostrar con los filtros actuales")
    
    # ==========================================
    # FOOTER
    # ==========================================
    
    st.markdown("---")
    st.markdown(
        "<p style='text-align: center; color: rgba(255,255,255,0.7);'>"
        "🏥 Dashboard Inteligencia en Salud Chile | Compra Ágil 2025 | "
        f"Datos actualizados: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        "</p>",
        unsafe_allow_html=True
    )
