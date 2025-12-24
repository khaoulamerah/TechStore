"""
Composants réutilisables pour les KPI Cards
Membre 4 : Frontend Developer
"""

import streamlit as st
import plotly.graph_objects as go

def create_metric_card(title, value, delta=None, delta_color="normal", icon=""):
    """
    Créer une carte KPI stylisée
    
    Args:
        title (str): Titre du KPI
        value (str/float): Valeur à afficher
        delta (str): Variation (optionnel)
        delta_color (str): Couleur du delta (normal/inverse/off)
        icon (str): Emoji ou icône
    """
    st.metric(
        label=f"{icon} {title}",
        value=value,
        delta=delta,
        delta_color=delta_color
    )

def create_gauge_chart(value, title, min_val=0, max_val=100, thresholds=None):
    """
    Créer un graphique de jauge (gauge)
    
    Args:
        value (float): Valeur actuelle
        title (str): Titre de la jauge
        min_val (float): Valeur minimale
        max_val (float): Valeur maximale
        thresholds (list): Liste de seuils [(val, color), ...]
    """
    if thresholds is None:
        thresholds = [
            (max_val * 0.3, "red"),
            (max_val * 0.6, "yellow"),
            (max_val, "green")
        ]
    
    steps = []
    prev_val = min_val
    for threshold, color in thresholds:
        steps.append({
            'range': [prev_val, threshold],
            'color': color
        })
        prev_val = threshold
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 20}},
        delta={'reference': max_val * 0.7},
        gauge={
            'axis': {'range': [min_val, max_val]},
            'bar': {'color': "darkblue"},
            'steps': steps,
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_val * 0.9
            }
        }
    ))
    
    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=50, b=20)
    )
    
    return fig

def create_comparison_card(metric_name, our_value, competitor_value, unit="DZD"):
    """
    Créer une carte de comparaison avec concurrence
    
    Args:
        metric_name (str): Nom de la métrique
        our_value (float): Notre valeur
        competitor_value (float): Valeur concurrent
        unit (str): Unité de mesure
    """
    difference = our_value - competitor_value
    pct_diff = (difference / competitor_value * 100) if competitor_value != 0 else 0
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(
            f"🏪 Notre {metric_name}",
            f"{our_value:,.0f} {unit}"
        )
    
    with col2:
        st.metric(
            f"🏬 Concurrent",
            f"{competitor_value:,.0f} {unit}",
            delta=f"{pct_diff:+.1f}%",
            delta_color="inverse" if difference > 0 else "normal"
        )
    
    if difference > 0:
        st.warning(f"⚠️ Nous sommes {abs(difference):,.0f} {unit} plus chers ({abs(pct_diff):.1f}%)")
    elif difference < 0:
        st.success(f"✅ Nous sommes {abs(difference):,.0f} {unit} moins chers ({abs(pct_diff):.1f}%)")
    else:
        st.info("💰 Prix identique à la concurrence")

def create_trend_indicator(current, previous, metric_name):
    """
    Créer un indicateur de tendance
    
    Args:
        current (float): Valeur actuelle
        previous (float): Valeur précédente
        metric_name (str): Nom de la métrique
    """
    if previous == 0:
        return
    
    change = ((current - previous) / previous) * 100
    
    if change > 0:
        st.success(f"📈 {metric_name} en hausse de {change:.1f}%")
    elif change < 0:
        st.error(f"📉 {metric_name} en baisse de {abs(change):.1f}%")
    else:
        st.info(f"➡️ {metric_name} stable")

def create_performance_badge(achievement_pct):
    """
    Créer un badge de performance basé sur l'atteinte des objectifs
    
    Args:
        achievement_pct (float): Pourcentage d'atteinte (0-100+)
    """
    if achievement_pct >= 100:
        badge = "🏆 Excellent"
        color = "green"
    elif achievement_pct >= 90:
        badge = "✅ Bon"
        color = "lightgreen"
    elif achievement_pct >= 75:
        badge = "⚠️ Moyen"
        color = "orange"
    else:
        badge = "❌ Faible"
        color = "red"
    
    st.markdown(f"""
    <div style='background-color: {color}; padding: 10px; border-radius: 5px; text-align: center;'>
        <h3 style='margin: 0; color: white;'>{badge}</h3>
        <p style='margin: 0; color: white; font-size: 20px;'>{achievement_pct:.1f}%</p>
    </div>
    """, unsafe_allow_html=True)