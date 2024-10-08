from flask import Flask, render_template, send_from_directory, url_for, request
from conecta_db import DB
import pandas as pd

app = Flask(__name__)
carre = DB()


def generate_tree(df, carregamento):
    tree_html = f'<div class="tree"><ul><li><a href="#">{carregamento}</a><ul>'
    for _, row in df.iterrows():
        if row["DESC_TECNICA"].endswith('EXP'):
            pop = str(row["LISTAGG"]).replace('|', '\n')
            tree_html += f'<li><a class="{row["TIPO_ORDEM"]}-link" href="{url_link(row["COD_ITEM"])}" ' \
                         f'title="{pop}">{row["NUM_ORDEM"]} - {row["COD_ITEM"]} - {row["QTDE"]}</a>'
            next_level_html = generate_next_level_tree(row["NUM_ORDEM"], carregamento)
            if next_level_html:
                tree_html += next_level_html
                tree_html += '</li>'
    tree_html += '</ul></div>'
    return tree_html


def generate_next_level_tree(num_ordem, carregamento):
    df_lv2 = pd.DataFrame(carre.filhos(carregamento, num_ordem))
    next_level_html = "<ul>"
    for _, row in df_lv2.iterrows():
        pop_set = set(str(row["LISTAGG"]).split('|'))
        pop = '\n'.join(pop_set)
        next_level_html += f'<li><a class="{row["TIPO_ORDEM"]}-link" href="{url_link(row["COD_ITEM"])}"title="{pop}">' \
                           f'{row["NUM_ORDEM"]} - {row["COD_ITEM"]} - {row["QTDE"]}</a>'
        next_level_html += generate_next_level_tree(row["NUM_ORDEM"], carregamento)
        next_level_html += "</li>"
    next_level_html += "</ul>"
    return next_level_html


def url_link(cod_item):
    pdf_filename = str(cod_item) + ".pdf"
    pdf_url = url_for("serve_pdf", filename=pdf_filename)
    return pdf_url


@app.route('/', methods=["GET", "POST"])
def criar():
    if request.method == "GET":
        return render_template('template.html')

    elif request.method == "POST":
        carregamento = request.form.get("carregamento")
        df_geral = carre.geral(carregamento)
        df_geral['QTDE'] = df_geral['QTDE'].astype(int)
        desc_car = df_geral.iloc[0, 0]
        desc_car = f'{desc_car} - CARREGAMENTO - {carregamento}'
        print(desc_car)
        car = carre.car(carregamento)
        df = pd.DataFrame(car)
        pop_up = carre.pop_up(carregamento)
        df = pd.merge(df, pop_up, on='NUM_ORDEM', how='inner')
        tree_html = generate_tree(df, carregamento)
        return render_template('template.html', tree_html=tree_html, desc_car=desc_car)


@app.route("/pdf/<filename>")
def serve_pdf(filename):
    pdf_directory = "\\\\10.40.3.5\\engenharia\\Engenharia\\06_Desenhos_PDF\\"
    return send_from_directory(pdf_directory, filename, as_attachment=False)


if __name__ == '__main__':
    try:
        app.run(host='0.0.0.0', port=8013)
    except Exception as err:
        print(err)
