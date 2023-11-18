import { useState } from "react";
import Spinner from "react-bootstrap/Spinner";
// import './App.css'

function EntradaRecomendacion({ categoria, subcategoria, products, id, base = "" }) {
  return (
    <div className="accordion-item">
      <h2 className="accordion-header">
        <button
          className="accordion-button collapsed"
          type="button"
          data-bs-toggle="collapse"
          data-bs-target={`#collapseEntrada${base}${id}`}
          aria-expanded="false"
          aria-controls={`collapseEntrada${base}${id}`}
        >
          {base !== "" ? "Recomendacion " + base + " #" + id : "Recomendacion #" + id}
        </button>
      </h2>
      <div

        id={`collapseEntrada${base}${id}`}
        className="accordion-collapse collapse"
      // data-bs-parent="#accordionExample2"
      >
        <div className="accordion-body">
          <ul>
            <li>Categoria: {categoria}</li>
            <li>Subcategoria: {subcategoria}</li>
            <ul>
              {products.map((product, index) => (
                <li key={index}>
                  {product["descripcion"]}
                  <ul>
                    <li >{`Codigo: ${product['codigosap']}`}</li>
                    <li >{`Estimación gasto: ${Math.round(product['est_total_gastado'] * 100) / 100}`}</li>
                  </ul>
                </li>
              ))}
            </ul>
            {/* <li>Productos: {products}</li> */}
          </ul>
        </div>
      </div>
    </div>
  );
}

function EntradaCompra({ categoria, subcategoria, total_gastado, id }) {
  return (
    <div className="accordion-item">
      <h2 className="accordion-header">
        <button
          className="accordion-button collapsed"
          type="button"
          data-bs-toggle="collapse"
          data-bs-target={`#collapseCompra${id}`}
          aria-controls={`collapseCompra${id}`}
          aria-expanded="false"
        >
          Compra #{id}
        </button>
      </h2>
      <div
        id={`collapseCompra${id}`}
        className="accordion-collapse collapse"
      // data-bs-parent="#accordionExample1"
      >
        <div className="accordion-body">
          <ul>
            <li>Categoria: {categoria}</li>
            <li>Subcategoria: {subcategoria}</li>
            <li>Total gastado: {total_gastado}</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

function App() {
  const [id, setId] = useState(0);
  const [compras, setCompras] = useState([]);
  const [recomendacionesALS, setRecomendacionesALS] = useState([]);
  const [recomendacionesCollaborative, setRecomendacionesCollaborative] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  const hacer_consulta = async (event) => {
    event.preventDefault();
    setIsLoading(true);

    try {
      const data_als = await (await fetch(`http://127.0.0.1:5000/als/${id}`)).json()
      setCompras(data_als["compras"]);
      setRecomendacionesALS(data_als["recomendaciones"]);

      const data_collaborative = await (await fetch(`http://127.0.0.1:5000/collaborative/${id}`)).json()
      setRecomendacionesCollaborative(data_collaborative["recomendaciones"]);

      console.log('a');
      setError("");

    } catch (error) {

      setError("Error al hacer la consulta");
    }

    setIsLoading(false);

  };

  return (
    <>
      <h1 style={{ marginLeft: "20px" }}>Ver Recomendaciones</h1>
      <form method="GET" className="row g-2" style={{ marginLeft: "20px", marginBottom: 20 }} onSubmit={hacer_consulta}>
        <div className="col-auto">
          <input
            className="form-control"
            id="id"
            type="number"
            value={id}
            onChange={(e) => setId(e.target.value)}
          />
        </div>
        <div className="col-auto">
          <input
            type="submit"
            className="btn btn-primary mb-2"
            value="Buscar Cliente"
          />
        </div>
        {
          isLoading &&
          <div className="col-auto">
            <Spinner animation="border" role="status" />
          </div>
        }
      </form>

      {

        error !== "" ?
          <div>{error}</div> :
          <div style={{ display: "flex" }}>

            <div style={{ flex: 1, marginLeft: 20, marginRight: 20 }} className="accordion" id="accordionExample1">
              {compras.map((compra, index) => (
                <EntradaCompra
                  key={index}
                  categoria={compra["categoria"]}
                  subcategoria={compra["subcategoria"]}
                  total_gastado={compra["total_gastado"]}
                  id={index + 1}
                />
              ))}
            </div>
            <div style={{ flex: 1, marginLeft: 20, marginRight: 20 }} className="accordion" id="accordionExample2">
              {recomendacionesALS.slice(0, 3).map((recomendacion, index) => (
                <EntradaRecomendacion
                  key={index}
                  categoria={recomendacion["categoria"]}
                  subcategoria={recomendacion["subcategoria"]}
                  products={recomendacion["products"]}
                  id={index + 1}
                  base="als"
                />
              ))}
            </div>

            <div style={{ flex: 1, marginLeft: 20, marginRight: 20 }} className="accordion" id="accordionExample2">
              {recomendacionesCollaborative.slice(0, 3).map((recomendacion, index) => (
                <EntradaRecomendacion
                  key={index}
                  categoria={recomendacion["categoria"]}
                  subcategoria={recomendacion["subcategoria"]}
                  products={recomendacion["products"]}
                  id={index + 1}
                  base="collaborative"
                />
              ))}
            </div>
          </div>

      }

    </>
  );
}

export default App;
